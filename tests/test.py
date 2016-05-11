import unittest
import zmq
from hedgehog.server import HedgehogServer, simulator
from hedgehog.client import HedgehogClient
from hedgehog.protocol import errors
from hedgehog.protocol.messages.motor import POWER, BRAKE, VELOCITY
from hedgehog.protocol.messages.process import STDOUT, STDERR


class TestClient(unittest.TestCase):
    def test_multiple_clients(self):
        context = zmq.Context()
        with HedgehogServer('inproc://controller', simulator.handler(), context=context), \
             HedgehogClient('inproc://controller', context=context) as client1, \
             HedgehogClient('inproc://controller', context=context) as client2:
            self.assertEqual(client1.get_analog(0), 0)
            self.assertEqual(client2.get_analog(0), 0)

    def test_unsupported(self):
        from hedgehog.server import handlers
        from hedgehog.server.handlers.hardware import HardwareHandler
        from hedgehog.server.handlers.process import ProcessHandler
        from hedgehog.server.hardware import HardwareAdapter

        context = zmq.Context()
        handlers = handlers.to_dict(HardwareHandler(HardwareAdapter()), ProcessHandler())
        with HedgehogServer('inproc://controller', handlers, context=context), \
             HedgehogClient('inproc://controller', context=context) as client:
            with self.assertRaises(errors.UnsupportedCommandError):
                client.get_analog(0)

    def test_get_analog(self):
        context = zmq.Context()
        with HedgehogServer('inproc://controller', simulator.handler(), context=context), \
             HedgehogClient('inproc://controller', context=context) as client:
                self.assertEqual(client.get_analog(0), 0)

    def test_set_analog_state(self):
        context = zmq.Context()
        with HedgehogServer('inproc://controller', simulator.handler(), context=context), \
             HedgehogClient('inproc://controller', context=context) as client:
            self.assertEqual(client.set_analog_state(0, False), None)

    def test_get_digital(self):
        context = zmq.Context()
        with HedgehogServer('inproc://controller', simulator.handler(), context=context), \
             HedgehogClient('inproc://controller', context=context) as client:
            self.assertEqual(client.get_digital(0), False)

    def test_set_digital_state(self):
        context = zmq.Context()
        with HedgehogServer('inproc://controller', simulator.handler(), context=context), \
             HedgehogClient('inproc://controller', context=context) as client:
            self.assertEqual(client.set_digital_state(0, False), None)

    def test_set_digital_output(self):
        context = zmq.Context()
        with HedgehogServer('inproc://controller', simulator.handler(), context=context), \
             HedgehogClient('inproc://controller', context=context) as client:
            self.assertEqual(client.set_digital_output(0, False), None)

    def test_set_motor(self):
        context = zmq.Context()
        with HedgehogServer('inproc://controller', simulator.handler(), context=context), \
             HedgehogClient('inproc://controller', context=context) as client:
            self.assertEqual(client.set_motor(0, POWER, 100), None)

    def test_get_motor(self):
        context = zmq.Context()
        with HedgehogServer('inproc://controller', simulator.handler(), context=context), \
             HedgehogClient('inproc://controller', context=context) as client:
            self.assertEqual(client.get_motor(0), (0, 0))

    def test_set_motor_position(self):
        context = zmq.Context()
        with HedgehogServer('inproc://controller', simulator.handler(), context=context), \
             HedgehogClient('inproc://controller', context=context) as client:
            self.assertEqual(client.set_motor_position(0, 0), None)

    def test_set_servo(self):
        context = zmq.Context()
        with HedgehogServer('inproc://controller', simulator.handler(), context=context), \
             HedgehogClient('inproc://controller', context=context) as client:
            self.assertEqual(client.set_servo(0, False, 0), None)

    def test_execute_process(self):
        context = zmq.Context()
        with HedgehogServer('inproc://controller', simulator.handler(), context=context), \
             HedgehogClient('inproc://controller', context=context) as client:
            on_exit_sock = context.socket(zmq.PAIR)
            on_exit_sock.bind('inproc://on_exit')

            process_info = {
                STDOUT: [],
                STDERR: [],
            }

            def on_stream(client, pid, fileno, chunk):
                process_info[fileno].append((pid, chunk))

            def on_exit(client, pid, exit_code):
                process_info['exit_pid'] = pid
                process_info['exit_code'] = exit_code

                on_exit_sock = context.socket(zmq.PAIR)
                on_exit_sock.connect('inproc://on_exit')
                on_exit_sock.send(b'')

            pid = client.execute_process('echo', 'asdf', stream_cb=on_stream, exit_cb=on_exit)

            on_exit_sock.recv()
            self.assertEqual(process_info['exit_pid'], pid)
            self.assertEqual(process_info['exit_code'], 0)
            for pid_, _ in process_info[STDOUT]:
                self.assertEqual(pid_, pid)
            for pid_, _ in process_info[STDERR]:
                self.assertEqual(pid_, pid)
            self.assertEqual(b''.join((chunk for _, chunk in process_info[STDOUT])), b'asdf\n')
            self.assertEqual(b''.join((chunk for _, chunk in process_info[STDERR])), b'')


if __name__ == '__main__':
    unittest.main()
