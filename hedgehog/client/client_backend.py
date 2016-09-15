import logging
import threading
import zmq
from hedgehog.utils.zmq.poller import Poller
from hedgehog.protocol import messages, sockets
from .client_handle import ClientHandle

_COMMAND = b'\x00'
_CONNECT = b'\x01'
_CLOSE = b'\x02'

logger = logging.getLogger(__name__)


class ClientBackend:
    def __init__(self, ctx, endpoint):
        self._ctx = zmq.Context()
        self.async_registries = {}

        socket = self._ctx.socket(zmq.ROUTER)
        socket.bind('inproc://socket')
        socket = sockets.DealerRouterWrapper(socket)

        backend = ctx.socket(zmq.DEALER)
        backend.connect(endpoint)
        backend = sockets.DealerRouterWrapper(backend)

        def socket_handler():
            # receive from the frontend
            header, [cmd, *msgs_raw] = socket.recv_multipart_raw()

            if cmd == _CONNECT:
                # send back the socket ID
                identity = header[0]
                self.async_registries[identity] = ClientHandle()
                socket.send_raw(header, identity)
            elif cmd == _CLOSE:
                # close the backend
                poller.unregister(socket.socket)
                poller.unregister(backend.socket)
            else:  # cmd == _COMMAND
                # forward to the backend
                assert len(msgs_raw) > 0
                backend.send_multipart(header, [messages.parse(msg) for msg in msgs_raw])

        def backend_handler():
            # receive from the backend
            header, msgs = backend.recv_multipart()
            assert len(msgs) > 0

            identity = header[0]
            async_registry = self.async_registries[identity]

            # either, all messages are replies corresponding to the previous requests,
            # or all messages are asynchronous updates
            if msgs[0].async:
                # handle asynchronous messages
                for msg in msgs:
                    async_registry.handle_async(self, msg)
            else:
                # handle synchronous messages
                async_registry.handle_register(self, msgs)
                socket.send_multipart(header, msgs)

        poller = Poller()
        poller.register(socket.socket, zmq.POLLIN, socket_handler)
        poller.register(backend.socket, zmq.POLLIN, backend_handler)

        def poll():
            while len(poller.sockets) > 0:
                for _, _, handler in poller.poll():
                    handler()

            socket.close()
            backend.close()

        threading.Thread(target=poll).start()

    def connect(self):
        socket = self._ctx.socket(zmq.REQ)
        socket.connect('inproc://socket')
        socket = sockets.ReqWrapper(socket)

        socket.send_raw(_CONNECT)
        identity = socket.recv_raw()
        return socket, self.async_registries[identity]

    def spawn(self, callback, *args, **kwargs):
        def target(*args, **kwargs):
            from . import HedgehogClient
            client = HedgehogClient._backend_new(self)
            callback(client, *args, **kwargs)

        threading.Thread(target=target, args=args, kwargs=kwargs).start()
