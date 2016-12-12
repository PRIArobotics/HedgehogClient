import logging
import random
import threading
import zmq

from hedgehog.protocol.messages import motor, servo
from hedgehog.protocol.messages.ack import Acknowledgement, FAILED_COMMAND
from hedgehog.protocol.sockets import ReqSocket, DealerRouterSocket
from hedgehog.utils.zmq.pipe import pipe, extended_pipe
from hedgehog.utils.zmq.poller import Poller
from .client_registry import ClientRegistry

logger = logging.getLogger(__name__)


class ClientBackend(object):
    def __init__(self, ctx, endpoint):
        self.ctx = ctx

        self.frontend = DealerRouterSocket(ctx, zmq.ROUTER).configure(hwm=1000)
        while True:
            self.endpoint = "inproc://frontend-%04x-%04x" % (random.randint(0, 0x10000), random.randint(0, 0x10000))
            try:
                self.frontend.bind(self.endpoint)
            except zmq.error.ZMQError as err:
                pass
            else:
                break

        self.backend = DealerRouterSocket(ctx, zmq.DEALER)
        self.backend.connect(endpoint)

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
        self._local = threading.local()

        self.poller = Poller()
        self.register_frontend()
        self.register_backend()
        self._shutdown = False

        threading.Thread(target=self.run).start()

    def shutdown(self):
        if not self._shutdown:
            self._shutdown = True
            self.registry.shutdown()
            self.backend.send_msgs([], [motor.Action(port, motor.POWER, 0) for port in range(0, 4)] +
                                       [servo.Action(port, False, 0) for port in range(0, 4)])

    def terminate(self):
        for socket in list(self.poller.sockets):
            self.poller.unregister(socket)

    def register_frontend(self):
        handlers = {}

        def command(cmd):
            return lambda func: handlers.update({cmd: func})

        def handle():
            header, [cmd, *msgs_raw] = self.frontend.recv_msgs_raw()
            handlers[cmd](header, *msgs_raw)

        self.poller.register(self.frontend, zmq.POLLIN, handle)

        @command(b'CONNECT')
        def handle_connect(header):
            client_handle = self.registry.connect(header[0])

            self.frontend.send_msg_raw(header, b'')
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
            self.frontend.send_msg_raw(header, b'')

        @command(b'SHUTDOWN')
        def handle_shutdown(header):
            self.shutdown()
            self.frontend.send_msg_raw(header, b'')

        @command(b'COMMAND')
        def handle_command(header, *msgs_raw):
            assert len(msgs_raw) > 0
            if self._shutdown:
                msgs = [Acknowledgement(FAILED_COMMAND, "Emergency Shutdown activated") for _ in msgs_raw]
                self.frontend.send_msgs(header, msgs)
            else:
                self.registry.prepare_register(header[0])
                self.backend.send_msgs_raw(header, msgs_raw)

    def register_backend(self):
        def handle():
            # receive from the backend
            header, msgs = self.backend.recv_msgs()
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
                self.frontend.send_msgs(header, msgs)

        self.poller.register(self.backend, zmq.POLLIN, handle)

    @property
    def client_handle(self):
        try:
            return self._local.client_handle
        except AttributeError:
            client_handle = self._connect()
            self._local.client_handle = client_handle
            return client_handle

    def _connect(self):
        socket = ReqSocket(self.ctx, zmq.REQ)
        socket.connect(self.endpoint)
        socket.send_msg_raw(b'CONNECT')
        socket.wait()
        self._pipe_frontend.wait()
        client_handle = self._pipe_frontend.pop()
        client_handle.socket = socket
        self._pipe_frontend.signal()
        return client_handle

    def spawn(self, callback, *args, daemon=False, async=False, **kwargs):
        if async:
            def signal(): pass

            def wait(): pass
        else:
            a, b = pipe(self.ctx)

            def signal():
                a.signal()
                a.close()

            def wait():
                b.wait()
                b.close()

        def target(*args, **kwargs):
            with self.client_handle:
                signal()
                self.client_handle.daemon = daemon
                callback(*args, **kwargs)

        threading.Thread(target=target, args=args, kwargs=kwargs).start()
        wait()

    def run(self):
        while len(self.poller.sockets) > 0:
            for _, _, handler in self.poller.poll():
                handler()
