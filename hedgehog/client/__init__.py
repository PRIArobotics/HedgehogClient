import threading
import zmq
from hedgehog.utils.zmq.poller import Poller
from hedgehog.protocol import errors, messages, sockets
from hedgehog.protocol.messages import ack, io, analog, digital, motor, servo, process
from .async import AsyncRegistry, MotorUpdateHandler, ProcessUpdateHandler


_COMMAND = b'\x00'
_CONNECT = b'\x01'
_CLOSE = b'\x02'


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
                self.async_registries[identity] = AsyncRegistry()
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
            client = HedgehogClient._backend_new(self)
            callback(client, *args, **kwargs)

        threading.Thread(target=target, args=args, kwargs=kwargs).start()


class HedgehogClient:
    def __init__(self, endpoint='tcp://127.0.0.1:10789', ctx=None):
        backend = ClientBackend(ctx, endpoint)
        self.__init(backend)

    @classmethod
    def _backend_new(cls, backend):
        self = cls.__new__(cls)
        self.__init(backend)
        return self

    def __init(self, backend):
        # TODO writes in the backend may interfere with this read
        self.socket, self.async_registry = backend.connect()

    def _send(self, msg, handler=None):
        reply = self._send_multipart((msg, handler))[0]
        if isinstance(reply, ack.Acknowledgement):
            if reply.code != ack.OK:
                raise errors.error(reply.code, reply.message)
            return None
        else:
            return reply

    def _send_multipart(self, *cmds):
        self.async_registry.new_handlers = [cmd[1] for cmd in cmds]
        self.socket.send_multipart_raw([_COMMAND] + [messages.serialize(cmd[0]) for cmd in cmds])
        return self.socket.recv_multipart()

    def set_input_state(self, port, pullup):
        self._send(io.StateAction(port, io.INPUT_PULLUP if pullup else io.INPUT_FLOATING))

    def get_analog(self, port):
        response = self._send(analog.Request(port))
        assert response.port == port
        return response.value

    def get_digital(self, port):
        response = self._send(digital.Request(port))
        assert response.port == port
        return response.value

    def set_digital_output(self, port, level):
        self._send(io.StateAction(port, io.OUTPUT_ON if level else io.OUTPUT_OFF))

    def set_motor(self, port, state, amount=0, reached_state=motor.POWER, relative=None, absolute=None, on_reached=None):
        if on_reached is not None:
            if relative is None and absolute is None:
                raise ValueError("callback given, but no end position")
            handler = MotorUpdateHandler(port, on_reached)
        else:
            handler = None
        self._send(motor.Action(port, state, amount, reached_state, relative, absolute), handler)

    def move(self, port, amount, state=motor.POWER):
        self.set_motor(port, state, amount)

    def move_relative_position(self, port, amount, relative, state=motor.POWER, on_reached=None):
        self.set_motor(port, state, amount, relative=relative, on_reached=on_reached)

    def move_absolute_position(self, port, amount, absolute, state=motor.POWER, on_reached=None):
        self.set_motor(port, state, amount, absolute=absolute, on_reached=on_reached)

    def get_motor(self, port):
        response = self._send(motor.Request(port))
        assert response.port == port
        return response.velocity, response.position

    def get_motor_velocity(self, port):
        velocity, _ = self.get_motor(port)
        return velocity

    def get_motor_position(self, port):
        _, position = self.get_motor(port)
        return position

    def set_motor_position(self, port, position):
        self._send(motor.SetPositionAction(port, position))

    def set_servo(self, port, active, position):
        self._send(servo.Action(port, active, position))

    def execute_process(self, *args, working_dir=None, on_stdout=None, on_stderr=None, on_exit=None):
        if on_stdout is not None or on_stderr is not None or on_exit is not None:
            handler = ProcessUpdateHandler(on_stdout, on_stderr, on_exit)
        else:
            handler = None
        response = self._send(process.ExecuteRequest(*args, working_dir=working_dir), handler)
        return response.pid

    def send_process_data(self, pid, chunk=b''):
        self._send(process.StreamAction(pid, process.STDIN, chunk))

    def close(self):
        self.socket.send_raw(_CLOSE)
        self.socket.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
