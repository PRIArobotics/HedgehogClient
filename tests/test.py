import unittest
import zmq
from hedgehog.server import HedgehogServer
from hedgehog.server.simulator import SimulatorCommandHandler
from hedgehog.client import HedgehogClient


class TestClient(unittest.TestCase):
    def test_get_analog(self):
        context = zmq.Context.instance()

        simulator = HedgehogServer('tcp://*:5555', SimulatorCommandHandler(), context=context)
        simulator.start()

        client = HedgehogClient('tcp://localhost:5555', context=context)
        self.assertEqual(client.get_analog(0), 0)
        client.close()

        simulator.close()


if __name__ == '__main__':
    unittest.main()
