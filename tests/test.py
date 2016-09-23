import unittest
import zmq
import time
from hedgehog.utils.zmq.pipe import pipe
from hedgehog.server import handlers, HedgehogServer
from hedgehog.server.handlers.hardware import HardwareHandler
from hedgehog.server.handlers.process import ProcessHandler
from hedgehog.server.hardware.simulated import SimulatedHardwareAdapter
from hedgehog.utils.discovery.service_node import ServiceNode
from hedgehog.client import HedgehogClient, find_server, get_client, entry_point
from hedgehog.protocol import errors
from hedgehog.protocol.messages.motor import POWER, BRAKE, VELOCITY
from hedgehog.protocol.messages.process import STDOUT, STDERR


def handler():
    return handlers.to_dict(HardwareHandler(SimulatedHardwareAdapter()), ProcessHandler())


class TestClient(unittest.TestCase):
    def test_multiple_clients(self):
        ctx = zmq.Context()
        with HedgehogServer(ctx, 'inproc://controller', handler()):
            with HedgehogClient(ctx, 'inproc://controller') as client1, \
                 HedgehogClient(ctx, 'inproc://controller') as client2:
                self.assertEqual(client1.get_analog(0), 0)
                self.assertEqual(client2.get_analog(0), 0)

    def test_unsupported(self):
        from hedgehog.server import handlers
        from hedgehog.server.handlers.hardware import HardwareHandler
        from hedgehog.server.handlers.process import ProcessHandler
        from hedgehog.server.hardware import HardwareAdapter

        ctx = zmq.Context()
        handlers = handlers.to_dict(HardwareHandler(HardwareAdapter()), ProcessHandler())
        with HedgehogServer(ctx, 'inproc://controller', handlers):
            with HedgehogClient(ctx, 'inproc://controller') as client:
                with self.assertRaises(errors.UnsupportedCommandError):
                    client.get_analog(0)

    def test_set_input_state(self):
        ctx = zmq.Context()
        with HedgehogServer(ctx, 'inproc://controller', handler()):
            with HedgehogClient(ctx, 'inproc://controller') as client:
                self.assertEqual(client.set_input_state(0, False), None)

    def test_get_analog(self):
        ctx = zmq.Context()
        with HedgehogServer(ctx, 'inproc://controller', handler()):
            with HedgehogClient(ctx, 'inproc://controller') as client:
                self.assertEqual(client.get_analog(0), 0)

    def test_get_digital(self):
        ctx = zmq.Context()
        with HedgehogServer(ctx, 'inproc://controller', handler()):
            with HedgehogClient(ctx, 'inproc://controller') as client:
                self.assertEqual(client.get_digital(0), False)

    def test_set_digital_output(self):
        ctx = zmq.Context()
        with HedgehogServer(ctx, 'inproc://controller', handler()):
            with HedgehogClient(ctx, 'inproc://controller') as client:
                self.assertEqual(client.set_digital_output(0, False), None)

    def test_set_motor(self):
        ctx = zmq.Context()
        with HedgehogServer(ctx, 'inproc://controller', handler()):
            with HedgehogClient(ctx, 'inproc://controller') as client:
                self.assertEqual(client.set_motor(0, POWER, 100), None)

    def test_get_motor(self):
        ctx = zmq.Context()
        with HedgehogServer(ctx, 'inproc://controller', handler()):
            with HedgehogClient(ctx, 'inproc://controller') as client:
                self.assertEqual(client.get_motor(0), (0, 0))

    def test_set_motor_position(self):
        ctx = zmq.Context()
        with HedgehogServer(ctx, 'inproc://controller', handler()):
            with HedgehogClient(ctx, 'inproc://controller') as client:
                self.assertEqual(client.set_motor_position(0, 0), None)

    def test_set_servo(self):
        ctx = zmq.Context()
        with HedgehogServer(ctx, 'inproc://controller', handler()):
            with HedgehogClient(ctx, 'inproc://controller') as client:
                self.assertEqual(client.set_servo(0, False, 0), None)

    def test_execute_process_no_streams(self):
        ctx = zmq.Context()
        with HedgehogServer(ctx, 'inproc://controller', handler()):
            with HedgehogClient(ctx, 'inproc://controller') as client:
                exit_a, exit_b = pipe(ctx)

                process_info = {
                }

                def on_exit(client, pid, exit_code):
                    process_info['exit'] = (pid, exit_code)

                    exit_b.send(b'')

                pid = client.execute_process('echo', 'asdf', on_exit=on_exit)
                client.send_process_data(pid)

                exit_a.recv()
                self.assertEqual(process_info['exit'], (pid, 0))

    def test_execute_process_one_stream(self):
        ctx = zmq.Context()
        with HedgehogServer(ctx, 'inproc://controller', handler()):
            with HedgehogClient(ctx, 'inproc://controller') as client:
                exit_a, exit_b = pipe(ctx)

                process_info = {
                    STDOUT: [],
                }

                def on_stream(client, pid, fileno, chunk):
                    time.sleep(0.1)
                    process_info[fileno].append((pid, chunk))

                def on_exit(client, pid, exit_code):
                    process_info['exit'] = (pid, exit_code)

                    exit_b.send(b'')

                pid = client.execute_process('echo', 'asdf', on_stdout=on_stream, on_exit=on_exit)
                client.send_process_data(pid)

                exit_a.recv()
                self.assertEqual(process_info['exit'], (pid, 0))
                for pid_, _ in process_info[STDOUT]:
                    self.assertEqual(pid_, pid)
                self.assertEqual(b''.join((chunk for _, chunk in process_info[STDOUT])), b'asdf\n')

    def test_execute_process_two_streams(self):
        ctx = zmq.Context()
        with HedgehogServer(ctx, 'inproc://controller', handler()):
            with HedgehogClient(ctx, 'inproc://controller') as client:
                exit_a, exit_b = pipe(ctx)

                process_info = {
                    STDOUT: [],
                    STDERR: [],
                }

                def on_stream(client, pid, fileno, chunk):
                    time.sleep(0.1)
                    process_info[fileno].append((pid, chunk))

                def on_exit(client, pid, exit_code):
                    process_info['exit'] = (pid, exit_code)

                    exit_b.send(b'')

                pid = client.execute_process('echo', 'asdf', on_stdout=on_stream, on_stderr=on_stream, on_exit=on_exit)
                client.send_process_data(pid)

                exit_a.recv()
                self.assertEqual(process_info['exit'], (pid, 0))
                for pid_, _ in process_info[STDOUT]:
                    self.assertEqual(pid_, pid)
                for pid_, _ in process_info[STDERR]:
                    self.assertEqual(pid_, pid)
                self.assertEqual(b''.join((chunk for _, chunk in process_info[STDOUT])), b'asdf\n')
                self.assertEqual(b''.join((chunk for _, chunk in process_info[STDERR])), b'')

    def test_shutdown(self):
        ctx = zmq.Context()
        with HedgehogServer(ctx, 'inproc://controller', handler()):
            with HedgehogClient(ctx, 'inproc://controller') as client:
                client.shutdown()
                with self.assertRaises(errors.FailedCommandError):
                    client.get_analog(0)

    def test_find_server(self):
        ctx = zmq.Context()
        node = ServiceNode(ctx, "Hedgehog Server")
        with node:
            SERVICE = 'hedgehog_server'

            node.join(SERVICE)
            node.add_service(SERVICE, 10789)

            server = find_server(ctx, SERVICE)
            port = list(server.services[SERVICE])[0].rsplit(':', 1)[1]

            self.assertEqual(port, "10789")

    def test_get_client(self):
        ctx = zmq.Context()
        with HedgehogServer(ctx, 'inproc://controller', handler()):
            with get_client('inproc://controller', ctx=ctx) as client:
                self.assertEqual(client.get_analog(0), 0)

    def test_entry_point(self):
        ctx = zmq.Context()
        with HedgehogServer(ctx, 'inproc://controller', handler()):
            @entry_point('inproc://controller', ctx=ctx)
            def main(client):
                self.assertEqual(client.get_analog(0), 0)

            main()


if __name__ == '__main__':
    unittest.main()
