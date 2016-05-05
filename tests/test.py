import unittest
import zmq
from hedgehog.server import HedgehogServer, simulator
from hedgehog.client import HedgehogClient
from hedgehog.protocol.messages.motor import POWER, BRAKE, VELOCITY


class TestClient(unittest.TestCase):
    def test_get_analog(self):
        context = zmq.Context()

        controller = HedgehogServer('inproc://controller', simulator.handler(), context=context)
        controller.start()

        client = HedgehogClient('inproc://controller', context=context)
        self.assertEqual(client.get_analog(0), 0)
        client.close()

        controller.close()

    def test_set_analog_state(self):
        context = zmq.Context()

        controller = HedgehogServer('inproc://controller', simulator.handler(), context=context)
        controller.start()

        client = HedgehogClient('inproc://controller', context=context)
        self.assertEqual(client.set_analog_state(0, False), None)
        client.close()

        controller.close()

    def test_get_digital(self):
        context = zmq.Context()

        controller = HedgehogServer('inproc://controller', simulator.handler(), context=context)
        controller.start()

        client = HedgehogClient('inproc://controller', context=context)
        self.assertEqual(client.get_digital(0), False)
        client.close()

        controller.close()

    def test_set_digital_state(self):
        context = zmq.Context()

        controller = HedgehogServer('inproc://controller', simulator.handler(), context=context)
        controller.start()

        client = HedgehogClient('inproc://controller', context=context)
        self.assertEqual(client.set_digital_state(0, False, False), None)
        client.close()

        controller.close()

    def test_set_digital_output(self):
        context = zmq.Context()

        controller = HedgehogServer('inproc://controller', simulator.handler(), context=context)
        controller.start()

        client = HedgehogClient('inproc://controller', context=context)
        self.assertEqual(client.set_digital_output(0, False), None)
        client.close()

        controller.close()

    def test_set_motor(self):
        context = zmq.Context()

        controller = HedgehogServer('inproc://controller', simulator.handler(), context=context)
        controller.start()

        client = HedgehogClient('inproc://controller', context=context)
        self.assertEqual(client.set_motor(0, POWER, 100), None)
        client.close()

        controller.close()

    def test_get_motor(self):
        context = zmq.Context()

        controller = HedgehogServer('inproc://controller', simulator.handler(), context=context)
        controller.start()

        client = HedgehogClient('inproc://controller', context=context)
        self.assertEqual(client.get_motor(0), (0, 0))
        client.close()

        controller.close()

    def test_set_motor_position(self):
        context = zmq.Context()

        controller = HedgehogServer('inproc://controller', simulator.handler(), context=context)
        controller.start()

        client = HedgehogClient('inproc://controller', context=context)
        self.assertEqual(client.set_motor_position(0, 0), None)
        client.close()

        controller.close()

    def test_set_servo(self):
        context = zmq.Context()

        controller = HedgehogServer('inproc://controller', simulator.handler(), context=context)
        controller.start()

        client = HedgehogClient('inproc://controller', context=context)
        self.assertEqual(client.set_servo(0, 0), None)
        client.close()

        controller.close()

    def test_set_servo_state(self):
        context = zmq.Context()

        controller = HedgehogServer('inproc://controller', simulator.handler(), context=context)
        controller.start()

        client = HedgehogClient('inproc://controller', context=context)
        self.assertEqual(client.set_servo_state(0, False), None)
        client.close()

        controller.close()


if __name__ == '__main__':
    unittest.main()
