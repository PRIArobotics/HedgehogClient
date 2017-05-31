from typing import Callable, Tuple

from hedgehog.protocol.messages import motor
from . import HedgehogClient


class Component(object):
    def __init__(self, hedgehog: HedgehogClient) -> None:
        self.hedgehog = hedgehog


class WithPort(Component):
    def __init__(self, hedgehog: HedgehogClient, port: int) -> None:
        super(WithPort, self).__init__(hedgehog)
        self.port = port


class Sensor(WithPort):
    def set_state(self, pullup: bool) -> None:
        self.hedgehog.set_input_state(self.port, pullup)

    def get_config(self) -> int:
        return self.hedgehog.get_io_config(self.port)


class AnalogSensor(Sensor):
    def get(self) -> int:
        return self.hedgehog.get_analog(self.port)


class DigitalSensor(Sensor):
    def get(self) -> bool:
        return self.hedgehog.get_digital(self.port)


class DigitalOutput(WithPort):
    def set(self, level):
        self.hedgehog.set_digital_output(self.port, level)

    def get_config(self) -> int:
        return self.hedgehog.get_io_config(self.port)


class Motor(WithPort):
    def set(self, state: int, amount: int=0,
            reached_state: int=motor.POWER, relative: int=None, absolute: int=None,
            on_reached: Callable[[int, int], None]=None) -> None:
        self.hedgehog.set_motor(self.port, state, amount, reached_state, relative, absolute, on_reached)

    def move(self, amount: int, state: int=motor.POWER) -> None:
        self.hedgehog.move(self.port, amount, state)

    def move_relative_position(self, amount: int, relative: int, state: int=motor.POWER,
                               on_reached: Callable[[int, int], None]=None) -> None:
        self.hedgehog.move_relative_position(self.port, amount, relative, state, on_reached)

    def move_absolute_position(self, amount: int, absolute: int, state: int=motor.POWER,
                               on_reached: Callable[[int, int], None]=None) -> None:
        self.hedgehog.move_absolute_position(self.port, amount, absolute, state, on_reached)

    def get_command(self) -> Tuple[int, int]:
        return self.hedgehog.get_motor_command(self.port)

    def get_state(self) -> Tuple[int, int]:
        return self.hedgehog.get_motor_state(self.port)

    def get_velocity(self) -> int:
        return self.hedgehog.get_motor_velocity(self.port)

    def get_position(self) -> int:
        return self.hedgehog.get_motor_position(self.port)

    def set_position(self, position: int) -> None:
        self.hedgehog.set_motor_position(self.port, position)


class Servo(WithPort):
    def set(self, active: bool, position: int) -> None:
        self.hedgehog.set_servo(self.port, active, position)

    def get_command(self) -> Tuple[int, int]:
        return self.hedgehog.get_servo_command(self.port)


class Process(Component):
    def __init__(self, hedgehog, pid):
        super(Process, self).__init__(hedgehog)
        self.pid = pid

    def signal(self, signal=2):
        self.hedgehog.signal_process(self.pid, signal)

    def send_data(self, chunk=b''):
        self.hedgehog.send_process_data(self.pid, chunk)


class HedgehogComponentGetterMixin(object):
    def analog(self, port):
        return AnalogSensor(self, port)

    def digital(self, port):
        return DigitalSensor(self, port)

    def output(self, port):
        return DigitalOutput(self, port)

    def motor(self, port):
        return Motor(self, port)

    def servo(self, port):
        return Servo(self, port)

    def process(self, *args, **kwargs):
        return Process(self, self.execute_process(*args, **kwargs))
