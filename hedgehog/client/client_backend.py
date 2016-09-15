import logging
import random
import threading
import zmq

from hedgehog.protocol import errors, messages, sockets
from hedgehog.utils.zmq.pipe import extended_pipe
from hedgehog.utils.zmq.poller import Poller
from hedgehog.utils.zmq.socket import Socket
from .client_handle import ClientHandle

logger = logging.getLogger(__name__)


class ClientBackend(object):
    def __init__(self, ctx, endpoint):
        self.ctx = ctx

        frontend = Socket(ctx, zmq.ROUTER).configure(hwm=1000)
        while True:
            self.endpoint = "inproc://frontend-%04x-%04x" % (random.randint(0, 0x10000), random.randint(0, 0x10000))
            try:
                frontend.bind(self.endpoint)
            except zmq.error.ZMQError as err:
                pass
            else:
                break
        self.frontend = sockets.DealerRouterWrapper(frontend)

        backend = ctx.socket(zmq.DEALER)
        backend.connect(endpoint)
        self.backend = sockets.DealerRouterWrapper(backend)

        # We need to give every client a handle to transmit out-of-band data. The simplest approach would be:
        #  Client: CONNECT / store handle in queue / Backend: OK / retrieve handle from queue
        # However, that would mean that multiple clients connecting at the same time could receive the wrong handle.
        # A fix would be this:
        #  Client: CONNECT / store handle in queue / Backend: OK / retrieve handle from queue / Client: OK
        # However, receiving the client's OK is problematic, as the backend may receive other messages before the OK.
        # The final approach uses a separate pipe, that is alternately shared between clients:
        #  Client: CONNECT / Backend: OK / client has exclusive access to pipe /
        #  Backend pipe: send handle / Client pipe: OK / client has no access to pipe
        # At any point, either zero or one clients have access to the pipe, eliminating the possibility of wrong message
        # order.
        self._pipe_backend, self._pipe_frontend = extended_pipe(ctx)

        self.clients = {}

        self.poller = Poller()
        self.register_frontend()
        self.register_backend()

        threading.Thread(target=self.run).start()

    def terminate(self):
        for socket in list(self.poller.sockets):
            self.poller.unregister(socket)

    def register_frontend(self):
        handlers = {}

        def command(cmd):
            return lambda func: handlers.update({cmd: func})

        def handle():
            header, [cmd, *msgs_raw] = self.frontend.recv_multipart_raw()
            handlers[cmd](header, *msgs_raw)

        self.poller.register(self.frontend.socket, zmq.POLLIN, handle)

        @command(b'CONNECT')
        def handle_connect(header):
            client_handle = ClientHandle()
            self.clients[header[0]] = client_handle

            self.frontend.send_raw(header, b'')
            self._pipe_backend.push(client_handle)
            self._pipe_backend.signal()
            self._pipe_backend.wait()

        @command(b'DISCONNECT')
        def handle_disconnect(header):
            del self.clients[header[0]]
            if len(self.clients) == 0:
                self.terminate()

        @command(b'COMMAND')
        def handle_command(header, *msgs_raw):
            assert len(msgs_raw) > 0
            self.backend.send_multipart(header, [messages.parse(msg) for msg in msgs_raw])

    def register_backend(self):
        def handle():
            # receive from the backend
            header, msgs = self.backend.recv_multipart()
            assert len(msgs) > 0

            client_handle = self.clients[header[0]]

            # either, all messages are replies corresponding to the previous requests,
            # or all messages are asynchronous updates
            if msgs[0].async:
                # handle asynchronous messages
                for msg in msgs:
                    client_handle.handle_async(self, msg)
            else:
                # handle synchronous messages
                client_handle.handle_register(self, msgs)
                self.frontend.send_multipart(header, msgs)

        self.poller.register(self.backend.socket, zmq.POLLIN, handle)

    def connect(self):
        socket = Socket(self.ctx, zmq.REQ)
        socket.connect(self.endpoint)
        socket = sockets.ReqWrapper(socket)
        socket.send_raw(b'CONNECT')
        socket.recv_raw()
        self._pipe_frontend.wait()
        client_handle = self._pipe_frontend.pop()
        self._pipe_frontend.signal()
        return socket, client_handle

    def spawn(self, callback, *args, **kwargs):
        def target(*args, **kwargs):
            from . import HedgehogClient
            client = HedgehogClient._backend_new(self)
            callback(client, *args, **kwargs)

        threading.Thread(target=target, args=args, kwargs=kwargs).start()

    def run(self):
        while len(self.poller.sockets) > 0:
            for _, _, handler in self.poller.poll():
                handler()
