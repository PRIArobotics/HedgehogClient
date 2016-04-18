import unittest
import zmq
from hedgehog.simulator import HedgehogSimulator
from hedgehog.client import HedgehogClient


class TestClient(unittest.TestCase):
    def test_get_analogs(self):
        context = zmq.Context.instance()

        simulator = HedgehogSimulator('tcp://*:5555', context=context)
        simulator.start()

        client = HedgehogClient('tcp://localhost:5555', context=context)
        self.assertEqual(client.get_analogs(0, 1), [0, 0])
        self.assertEqual(client.get_analog(0), 0)
        client.close()

        simulator.close()


if __name__ == '__main__':
    unittest.main()
