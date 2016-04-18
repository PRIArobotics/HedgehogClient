import unittest
import zmq
from hedgehog.simulator import HedgehogSimulator
from hedgehog.client.controller import HedgehogController


class TestClient(unittest.TestCase):
    def test_get_analogs(self):
        context = zmq.Context.instance()

        simulator = HedgehogSimulator('tcp://*:5555', context=context)
        simulator.start()

        controller = HedgehogController('tcp://localhost:5555', b'client')
        self.assertEqual(controller.get_analogs(0, 1), [0, 0])
        self.assertEqual(controller.get_analog(0), 0)

        controller.close()
        simulator.kill()


if __name__ == '__main__':
    unittest.main()
