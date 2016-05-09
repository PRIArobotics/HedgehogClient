import threading
import zmq
from hedgehog.protocol import messages, sockets
from hedgehog.protocol.messages import ack, analog, digital, motor, servo, process


_COMMAND = b'\x00'
_CONNECT = b'\x01'
_CLOSE = b'\x02'


class ClientData:
    def __init__(self):
        self.out_of_band = None
        self.motor_cbs = {}
        self.process_cbs = {}

    def handle_out_of_band(self, reps):
        assert len(self.out_of_band) == len(reps)
        for handler, rep in zip(self.out_of_band, reps):
            if handler is not None:
                handler(rep)
        self.out_of_band = None


class ClientBackend:
    def __init__(self, endpoint, context=None):
        self._context = zmq.Context()
        self.context = context or zmq.Context.instance()
        self.endpoint = endpoint
        self.client_data = {}

        signal = self._context.socket(zmq.PAIR)
        signal.bind('inproc://signal')

        threading.Thread(target=self.run).start()

        signal.recv()
        signal.close()

    def run(self):
        socket = self._context.socket(zmq.ROUTER)
        socket.bind('inproc://socket')
        socket = sockets.DealerRouterWrapper(socket)

        backend = self.context.socket(zmq.DEALER)
        backend.connect(self.endpoint)
        backend = sockets.DealerRouterWrapper(backend)

        signal = self._context.socket(zmq.PAIR)
        signal.connect('inproc://signal')
        signal.send(b'')
        signal.close()

        poller = zmq.Poller()
        poller.register(socket.socket, zmq.POLLIN)
        poller.register(backend.socket, zmq.POLLIN)
        while len(poller.sockets) > 0:
            for sock, _ in poller.poll():
                if sock is socket.socket:
                    # receive from the frontend
                    header, [cmd, *msgs_raw] = socket.recv_multipart_raw()

                    if cmd == _CONNECT:
                        # send back the socket ID
                        identity = header[0]
                        self.client_data[identity] = ClientData()
                        socket.send_raw(header, identity)
                    elif cmd == _CLOSE:
                        # close the backend
                        poller.unregister(socket.socket)
                        poller.unregister(backend.socket)
                    else:  # cmd == _COMMAND
                        # forward to the backend
                        assert len(msgs_raw) > 0
                        backend.send_multipart(header, [messages.parse(msg) for msg in msgs_raw])
                else:  # sock is backend.socket
                    # receive from the backend
                    header, msgs = backend.recv_multipart()
                    assert len(msgs) > 0

                    identity = header[0]
                    client_data = self.client_data[identity]

                    # either, all messages are replies corresponding to the previous requests,
                    # or all messages are asynchronous updates
                    if msgs[0].async:
                        # handle asynchronous messages
                        for msg in msgs:
                            if type(msg) is messages.motor.StateUpdate:
                                reached_cb, = client_data.process_cbs[msg.pid]
                                if reached_cb is not None:
                                    self.spawn(reached_cb, msg.port, msg.state)
                            if type(msg) is messages.process.StreamUpdate:
                                stream_cb, _ = client_data.process_cbs[msg.pid]
                                if stream_cb is not None:
                                    self.spawn(stream_cb, msg.pid, msg.fileno, msg.chunk)
                            if type(msg) is messages.process.ExitUpdate:
                                _, exit_cb = client_data.process_cbs[msg.pid]
                                if exit_cb is not None:
                                    self.spawn(exit_cb, msg.pid, msg.exit_code)
                    else:
                        # handle synchronous messages
                        client_data.handle_out_of_band(msgs)
                        socket.send_multipart(header, msgs)


        socket.close()
        backend.close()

    def connect(self):
        socket = self._context.socket(zmq.REQ)
        socket.connect('inproc://socket')

        return _HedgehogClient(self, socket)

    def spawn(self, callback, *args, **kwargs):
        def target(*args, **kwargs):
            client = self.connect()
            callback(client, *args, **kwargs)

        threading.Thread(target=target, args=args, kwargs=kwargs).start()


def HedgehogClient(endpoint, context=None):
    backend = ClientBackend(endpoint, context=context)
    return backend.connect()


class _HedgehogClient:
    def __init__(self, backend, socket):
        self.socket = sockets.ReqWrapper(socket)

        self.socket.send_raw(_CONNECT)
        identity = self.socket.recv_raw()

        # TODO writes in the backend may interfere with this read
        self.client_data = backend.client_data[identity]

    def _send(self, msg, handler=None):
        return self._send_multipart((msg, handler))[0]

    def _send_multipart(self, *cmds):
        self.client_data.out_of_band = [cmd[1] for cmd in cmds]
        self.socket.send_multipart_raw([_COMMAND] + [cmd[0].serialize() for cmd in cmds])
        return self.socket.recv_multipart()

    def get_analog(self, port):
        response = self._send(analog.Request(port))
        assert response.port == port
        return response.value

    def set_analog_state(self, port, pullup):
        response = self._send(analog.StateAction(port, pullup))
        assert response.code == ack.OK

    def get_digital(self, port):
        response = self._send(digital.Request(port))
        assert response.port == port
        return response.value

    def set_digital_state(self, port, pullup, output):
        response = self._send(digital.StateAction(port, pullup, output))
        assert response.code == ack.OK

    def set_digital_output(self, port, level):
        response = self._send(digital.Action(port, level))
        assert response.code == ack.OK

    def set_motor(self, port, state, amount=0, reached_state=motor.POWER, relative=None, absolute=None, reached_cb=None):
        if reached_cb is not None and relative is None and absolute is None:
            raise ValueError("callback given, but no end position")
        def register(rep):
            self.client_data.motor_cbs[port] = (reached_cb,)
        response = self._send(motor.Action(port, state, amount, reached_state, relative, absolute), register)
        assert response.code == ack.OK

    def move(self, port, amount, state=motor.POWER):
        self.set_motor(port, state, amount)

    def move_relative_position(self, port, amount, relative, state=motor.POWER, reached_cb=None):
        self.set_motor(port, state, amount, relative=relative, reached_cb=reached_cb)

    def move_absolute_position(self, port, amount, absolute, state=motor.POWER, reached_cb=None):
        self.set_motor(port, state, amount, absolute=absolute, reached_cb=reached_cb)

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
        response = self._send(motor.SetPositionAction(port, position))
        assert response.code == ack.OK

    def set_servo(self, port, position):
        response = self._send(servo.Action(port, position))
        assert response.code == ack.OK

    def set_servo_state(self, port, active):
        response = self._send(servo.StateAction(port, active))
        assert response.code == ack.OK

    def execute_process(self, *args, working_dir=None, stream_cb=None, exit_cb=None):
        def register(rep):
            self.client_data.process_cbs[rep.pid] = (stream_cb, exit_cb)
        response = self._send(process.ExecuteRequest(*args, working_dir=working_dir), register)
        return response.pid

    def send_process_data(self, pid, chunk=b''):
        response = self._send(process.StreamAction(pid, process.STDIN, chunk))
        assert response.code == ack.OK

    def close(self):
        self.socket.send_raw(_CLOSE)
        self.socket.close()
