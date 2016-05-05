import unittest
import zmq
from hedgehog.server import HedgehogServer, simulator
from hedgehog.client import HedgehogClient


class TestClient(unittest.TestCase):
    def test_get_analog(self):
        context = zmq.Context.instance()

        controller = HedgehogServer('tcp://*:5555', simulator.handler(), context=context)
        controller.start()

        client = HedgehogClient('tcp://localhost:5555', context=context)
        self.assertEqual(client.get_analog(0), 0)
        client.close()

        controller.close()


if __name__ == '__main__':
    unittest.main()
