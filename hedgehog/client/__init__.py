import threading
import zmq
from hedgehog.protocol import messages, sockets
from hedgehog.protocol.messages import ack, analog, digital, motor, servo, process


_COMMAND = b'\x00'
_CLOSE = b'\x01'

class ClientBackend:
    def __init__(self, endpoint, context=None):
        self._context = zmq.Context()
        self.context = context or zmq.Context.instance()
        self.endpoint = endpoint

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
                    header, msgs_raw = socket.recv_multipart_raw()
                    type = msgs_raw[0]

                    if type == _CLOSE:
                        # close the backend
                        poller.unregister(socket.socket)
                        poller.unregister(backend.socket)
                    else:  # type == _COMMAND
                        # forward to the backend
                        backend.send_multipart(header, [messages.parse(msg) for msg in msgs_raw[1:]])
                else:  # sock is backend.socket
                    # receive from the backend
                    header, msgs = backend.recv_multipart()

                    # handle synchronous messages
                    socket.send_multipart(header, [msg for msg in msgs if not msg.async])
                    # handle asynchronous messages
                    for msg in msgs:
                        if msg.async:
                            # currently motor.StateUpdate, process.StreamUpdate or process.ExitUpdate
                            # TODO
                            pass
        socket.close()
        backend.close()

    def connect(self):
        socket = self._context.socket(zmq.REQ)
        socket.connect('inproc://socket')

        return _HedgehogClient(socket)

    def spawn(self, callback):
        def target():
            client = self.connect()
            callback(client)

        threading.Thread(target=target).start()


def HedgehogClient(endpoint, context= None):
    backend = ClientBackend(endpoint, context=context)
    return backend.connect()


class _HedgehogClient:
    def __init__(self, socket):
        self.socket = sockets.ReqWrapper(socket)

    def _send(self, msg):
        self.socket.send_multipart_raw([_COMMAND, msg.serialize()])
        return self.socket.recv()

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
        response = self._send(motor.Action(port, state, amount, reached_state, relative, absolute))
        assert response.code == ack.OK

    def move(self, port, amount, state=motor.POWER):
        self.set_motor(port, state, amount)

    def move_relative_position(self, port, amount, relative, state=motor.POWER, reached_cb=None):
        self.set_motor(port, state, amount, relative=relative)

    def move_absolute_position(self, port, amount, absolute, state=motor.POWER, reached_cb=None):
        self.set_motor(port, state, amount, absolute=absolute)

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
        response = self._send(process.ExecuteRequest(*args, working_dir=working_dir))
        assert response.code == ack.OK
        return response.pid

    def send_process_data(self, pid, chunk=b''):
        response = self._send(process.StreamAction(pid, process.STDIN, chunk))
        assert response.code == ack.OK

    def close(self):
        self.socket.send_raw(_CLOSE)
        self.socket.close()
