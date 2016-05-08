import threading
import zmq
from hedgehog.protocol import sockets
from hedgehog.protocol.messages import ack, analog, digital, motor, servo, process


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

        closer = self._context.socket(zmq.ROUTER)
        closer.bind('inproc://closer')

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
        poller.register(closer, zmq.POLLIN)
        while len(poller.sockets) > 0:
            for sock, _ in poller.poll():
                if sock is closer:
                    sock.recv()
                    poller.unregister(socket.socket)
                    poller.unregister(backend.socket)
                    poller.unregister(closer)
                else:
                    input, output = (socket, backend) if sock is socket.socket else (backend, socket)
                    header, msgs = input.recv_multipart()
                    output.send_multipart(header, msgs)
        socket.close()
        backend.close()
        closer.close()

    def connect(self):
        socket = self._context.socket(zmq.REQ)
        socket.connect('inproc://socket')

        closer= self._context.socket(zmq.DEALER)
        closer.connect('inproc://closer')
        return _HedgehogClient(socket, closer)


def HedgehogClient(endpoint, context= None):
    backend = ClientBackend(endpoint, context=context)
    return backend.connect()


class _HedgehogClient:
    def __init__(self, socket, closer):
        self.socket = sockets.ReqWrapper(socket)
        self.closer = closer

    def get_analog(self, port):
        self.socket.send(analog.Request(port))
        response = self.socket.recv()
        assert response.port == port
        return response.value

    def set_analog_state(self, port, pullup):
        self.socket.send(analog.StateAction(port, pullup))
        response = self.socket.recv()
        assert response.code == ack.OK

    def get_digital(self, port):
        self.socket.send(digital.Request(port))
        response = self.socket.recv()
        assert response.port == port
        return response.value

    def set_digital_state(self, port, pullup, output):
        self.socket.send(digital.StateAction(port, pullup, output))
        response = self.socket.recv()
        assert response.code == ack.OK

    def set_digital_output(self, port, level):
        self.socket.send(digital.Action(port, level))
        response = self.socket.recv()
        assert response.code == ack.OK

    def set_motor(self, port, state, amount=0, reached_state=motor.POWER, relative=None, absolute=None):
        self.socket.send(motor.Action(port, state, amount, reached_state, relative, absolute))
        response = self.socket.recv()
        assert response.code == ack.OK

    def move(self, port, amount, state=motor.POWER):
        self.set_motor(port, state, amount)

    def move_relative_position(self, port, amount, relative, state=motor.POWER):
        self.set_motor(port, state, amount, relative=relative)

    def move_absolute_position(self, port, amount, absolute, state=motor.POWER):
        self.set_motor(port, state, amount, absolute=absolute)

    def get_motor(self, port):
        self.socket.send(motor.Request(port))
        response = self.socket.recv()
        assert response.port == port
        return response.velocity, response.position

    def get_motor_velocity(self, port):
        velocity, _ = self.get_motor(port)
        return velocity

    def get_motor_position(self, port):
        _, position = self.get_motor(port)
        return position

    def set_motor_position(self, port, position):
        self.socket.send(motor.SetPositionAction(port, position))
        response = self.socket.recv()
        assert response.code == ack.OK

    def set_servo(self, port, position):
        self.socket.send(servo.Action(port, position))
        response = self.socket.recv()
        assert response.code == ack.OK

    def set_servo_state(self, port, active):
        self.socket.send(servo.StateAction(port, active))
        response = self.socket.recv()
        assert response.code == ack.OK

    def close(self):
        self.closer.send(b'')
        self.closer.close()
        self.socket.close()
