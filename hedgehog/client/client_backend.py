import logging
import random
import threading
import zmq

from hedgehog.protocol import messages, sockets
from hedgehog.protocol.messages import motor, servo
from hedgehog.protocol.messages.ack import Acknowledgement, FAILED_COMMAND
from hedgehog.utils.zmq.pipe import extended_pipe
from hedgehog.utils.zmq.poller import Poller
from hedgehog.utils.zmq.socket import Socket
from .client_registry import ClientRegistry

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

        self.registry = ClientRegistry()

        self.poller = Poller()
        self.register_frontend()
        self.register_backend()
        self._shutdown = False

        threading.Thread(target=self.run).start()

    def shutdown(self):
        if not self._shutdown:
            self._shutdown = True
            self.registry.shutdown()
            self.backend.send_multipart([], [motor.Action(port, motor.POWER, 0) for port in range(0, 4)] +
                                        [servo.Action(port, False, 0) for port in range(0, 4)])

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
            client_handle = self.registry.connect(header[0])

            self.frontend.send_raw(header, b'')
            self._pipe_backend.push(client_handle)
            self._pipe_backend.signal()
            self._pipe_backend.wait()

        @command(b'DISCONNECT')
        def handle_disconnect(header):
            self.registry.disconnect(header[0])
            if all(client.daemon for client in self.registry.clients.values()):
                self.shutdown()
            if len(self.registry.clients) == 0:
                self.terminate()
            self.frontend.send_raw(header, b'')

        @command(b'SHUTDOWN')
        def handle_shutdown(header):
            self.shutdown()
            self.frontend.send_raw(header, b'')

        @command(b'COMMAND')
        def handle_command(header, *msgs_raw):
            assert len(msgs_raw) > 0
            if self._shutdown:
                msgs = [Acknowledgement(FAILED_COMMAND, "Emergency Shutdown activated") for _ in msgs_raw]
                self.frontend.send_multipart(header, msgs)
            else:
                self.registry.prepare_register(header[0])
                self.backend.send_multipart(header, [messages.parse(msg) for msg in msgs_raw])

    def register_backend(self):
        def handle():
            # receive from the backend
            header, msgs = self.backend.recv_multipart()
            assert len(msgs) > 0
            if len(header) == 0:
                # sent by the backend for shutdown, ignore
                return

            # either, all messages are replies corresponding to the previous requests,
            # or all messages are asynchronous updates
            if msgs[0].async:
                # handle asynchronous messages
                for msg in msgs:
                    self.registry.handle_async(msg)
            else:
                # handle synchronous messages
                self.registry.handle_register(header[0], self, msgs)
                self.frontend.send_multipart(header, msgs)

        self.poller.register(self.backend.socket, zmq.POLLIN, handle)

    def _connect(self, daemon):
        socket = Socket(self.ctx, zmq.REQ)
        socket.connect(self.endpoint)
        socket = sockets.ReqWrapper(socket)
        socket.send_raw(b'CONNECT')
        socket.recv_raw()
        self._pipe_frontend.wait()
        client_handle = self._pipe_frontend.pop()
        client_handle.daemon = daemon
        self._pipe_frontend.signal()
        return socket, client_handle

    def spawn(self, callback, *args, daemon=False, **kwargs):
        def target(*args, **kwargs):
            from . import HedgehogClient
            with HedgehogClient._backend_new(self, daemon) as client:
                callback(client, *args, **kwargs)

        threading.Thread(target=target, args=args, kwargs=kwargs).start()

    def run(self):
        while len(self.poller.sockets) > 0:
            for _, _, handler in self.poller.poll():
                handler()
