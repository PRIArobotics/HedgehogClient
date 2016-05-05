import zmq
from hedgehog.protocol import sockets
from hedgehog.protocol.messages import ack, analog, digital, motor, servo


class HedgehogClient:
    def __init__(self, endpoint, context=None):
        context = context or zmq.Context.instance()
        socket = context.socket(zmq.DEALER)
        socket.connect(endpoint)
        self.socket = sockets.DealerWrapper(socket)

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
        self.socket.close()
