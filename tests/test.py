import threading
import traceback
import unittest

import zmq
import time
from hedgehog.utils.zmq.pipe import pipe
from hedgehog.utils.zmq.socket import Socket
from hedgehog.server import handlers, HedgehogServer
from hedgehog.server.handlers.hardware import HardwareHandler
from hedgehog.server.handlers.process import ProcessHandler
from hedgehog.server.hardware.simulated import SimulatedHardwareAdapter
from hedgehog.utils.discovery.service_node import ServiceNode
from hedgehog.client import HedgehogClient, find_server, get_client, entry_point
from hedgehog.protocol import errors, sockets
from hedgehog.protocol.messages import ack, analog, digital, io, motor, servo, process
from hedgehog.protocol.messages.motor import POWER, BRAKE, VELOCITY
from hedgehog.protocol.messages.process import STDOUT, STDERR


def handler():
    return handlers.to_dict(HardwareHandler(SimulatedHardwareAdapter()), ProcessHandler())


class HedgehogServerDummy(object):
    def __init__(self, testcase, ctx, endpoint):
        socket = Socket(ctx, zmq.ROUTER)
        socket.bind(endpoint)
        self.socket = sockets.DealerRouterWrapper(socket)
        self.testcase = testcase

    def __call__(self, func):
        def target():
            try:
                func(self)

                ident, msgs = self.socket.recv_multipart()
                self.testcase.assertEqual(msgs,
                                          [motor.Action(port, motor.POWER, 0) for port in range(0, 4)] +
                                          [servo.Action(port, False, 0) for port in range(0, 4)])
                self.socket.send_multipart(ident, [ack.Acknowledgement() for i in range(0, 8)])

                func.exc = None
            except Exception as exc:
                traceback.print_exc()
                func.exc = exc

        thread = threading.Thread(target=target, name=traceback.extract_stack(limit=2)[0].name)
        thread.start()

        def join():
            thread.join()
            if func.exc is not None:
                raise func.exc

        func.join = join
        return func


class TestHedgehogClient(unittest.TestCase):
    def test_connect(self):
        ctx = zmq.Context()

        @HedgehogServerDummy(self, ctx, 'inproc://controller')
        def thread(server):
            pass

        with HedgehogClient(ctx, 'inproc://controller') as client:
            pass

        thread.join()

    def test_multiple_client_threads(self):
        ctx = zmq.Context()

        @HedgehogServerDummy(self, ctx, 'inproc://controller')
        def thread(server):
            ident1, msg = server.socket.recv()
            self.assertEqual(msg, analog.Request(0))
            server.socket.send(ident1, analog.Update(0, 0))

            ident2, msg = server.socket.recv()
            self.assertEqual(msg, analog.Request(0))
            server.socket.send(ident2, analog.Update(0, 0))

            self.assertEqual(ident1[0], ident2[0])

        with HedgehogClient(ctx, 'inproc://controller') as client1:
            self.assertEqual(client1.get_analog(0), 0)

            def spawned(client2):
                self.assertEqual(client2.get_analog(0), 0)

            client1.spawn(spawned)

        thread.join()

    def test_unsupported(self):
        ctx = zmq.Context()

        @HedgehogServerDummy(self, ctx, 'inproc://controller')
        def thread(server):
            ident, msg = server.socket.recv()
            self.assertEqual(msg, analog.Request(0))
            server.socket.send(ident, ack.Acknowledgement(ack.UNSUPPORTED_COMMAND))

        with HedgehogClient(ctx, 'inproc://controller') as client:
            with self.assertRaises(errors.UnsupportedCommandError):
                client.get_analog(0)

        thread.join()

    def test_shutdown(self):
        ctx = zmq.Context()

        @HedgehogServerDummy(self, ctx, 'inproc://controller')
        def thread(server):
            pass

        with HedgehogClient(ctx, 'inproc://controller') as client:
            client.shutdown()
            with self.assertRaises(errors.FailedCommandError):
                client.get_analog(0)

        thread.join()


class TestClientConvenienceFunctions(unittest.TestCase):
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

    def test_entry_point_with_emergency_shutdown(self):
        ctx = zmq.Context()
        with HedgehogServer(ctx, 'inproc://controller', handler()):
            @entry_point('inproc://controller', emergency=0, ctx=ctx)
            def main(client):
                self.assertEqual(client.get_analog(0), 0)

            main()


class TestHedgehogClientAPI(unittest.TestCase):
    def test_set_input_state(self):
        ctx = zmq.Context()

        @HedgehogServerDummy(self, ctx, 'inproc://controller')
        def thread(server):
            ident, msg = server.socket.recv()
            self.assertEqual(msg, io.StateAction(0, io.INPUT_FLOATING))
            server.socket.send(ident, ack.Acknowledgement())

        with HedgehogClient(ctx, 'inproc://controller') as client:
            self.assertEqual(client.set_input_state(0, False), None)

        thread.join()

    def test_get_analog(self):
        ctx = zmq.Context()

        @HedgehogServerDummy(self, ctx, 'inproc://controller')
        def thread(server):
            ident, msg = server.socket.recv()
            self.assertEqual(msg, analog.Request(0))
            server.socket.send(ident, analog.Update(0, 0))

        with HedgehogClient(ctx, 'inproc://controller') as client:
            self.assertEqual(client.get_analog(0), 0)

        thread.join()

    def test_get_digital(self):
        ctx = zmq.Context()

        @HedgehogServerDummy(self, ctx, 'inproc://controller')
        def thread(server):
            ident, msg = server.socket.recv()
            self.assertEqual(msg, digital.Request(0))
            server.socket.send(ident, digital.Update(0, False))

        with HedgehogClient(ctx, 'inproc://controller') as client:
            self.assertEqual(client.get_digital(0), False)

        thread.join()

    def test_set_digital_output(self):
        ctx = zmq.Context()

        @HedgehogServerDummy(self, ctx, 'inproc://controller')
        def thread(server):
            ident, msg = server.socket.recv()
            self.assertEqual(msg, io.StateAction(0, io.OUTPUT_OFF))
            server.socket.send(ident, ack.Acknowledgement())

        with HedgehogClient(ctx, 'inproc://controller') as client:
            self.assertEqual(client.set_digital_output(0, False), None)

        thread.join()

    def test_set_motor(self):
        ctx = zmq.Context()

        @HedgehogServerDummy(self, ctx, 'inproc://controller')
        def thread(server):
            ident, msg = server.socket.recv()
            self.assertEqual(msg, motor.Action(0, POWER, 100))
            server.socket.send(ident, ack.Acknowledgement())

        with HedgehogClient(ctx, 'inproc://controller') as client:
            self.assertEqual(client.set_motor(0, POWER, 100), None)

        thread.join()

    def test_get_motor(self):
        ctx = zmq.Context()

        @HedgehogServerDummy(self, ctx, 'inproc://controller')
        def thread(server):
            ident, msg = server.socket.recv()
            self.assertEqual(msg, motor.Request(0))
            server.socket.send(ident, motor.Update(0, 0, 0))

        with HedgehogClient(ctx, 'inproc://controller') as client:
            self.assertEqual(client.get_motor(0), (0, 0))

        thread.join()

    def test_set_motor_position(self):
        ctx = zmq.Context()

        @HedgehogServerDummy(self, ctx, 'inproc://controller')
        def thread(server):
            ident, msg = server.socket.recv()
            self.assertEqual(msg, motor.SetPositionAction(0, 0))
            server.socket.send(ident, ack.Acknowledgement())

        with HedgehogClient(ctx, 'inproc://controller') as client:
            self.assertEqual(client.set_motor_position(0, 0), None)

        thread.join()

    def test_set_servo(self):
        ctx = zmq.Context()

        @HedgehogServerDummy(self, ctx, 'inproc://controller')
        def thread(server):
            ident, msg = server.socket.recv()
            self.assertEqual(msg, servo.Action(0, False, 0))
            server.socket.send(ident, ack.Acknowledgement())

        with HedgehogClient(ctx, 'inproc://controller') as client:
            self.assertEqual(client.set_servo(0, False, 0), None)

        thread.join()

    def test_execute_process_no_handlers(self):
        ctx = zmq.Context()

        @HedgehogServerDummy(self, ctx, 'inproc://controller')
        def thread(server):
            ident, msg = server.socket.recv()
            self.assertEqual(msg, process.ExecuteRequest('echo', 'asdf'))
            server.socket.send(ident, process.ExecuteReply(2345))
            server.socket.send(ident, process.StreamUpdate(2345, process.STDOUT, b'asdf\n'))
            server.socket.send(ident, process.StreamUpdate(2345, process.STDOUT))
            server.socket.send(ident, process.StreamUpdate(2345, process.STDERR))
            server.socket.send(ident, process.ExitUpdate(2345, 0))

        with HedgehogClient(ctx, 'inproc://controller') as client:
            self.assertEqual(client.execute_process('echo', 'asdf'), 2345)

        thread.join()

    def test_execute_process_handle_exit(self):
        ctx = zmq.Context()

        @HedgehogServerDummy(self, ctx, 'inproc://controller')
        def thread(server):
            ident, msg = server.socket.recv()
            self.assertEqual(msg, process.ExecuteRequest('echo', 'asdf'))
            server.socket.send(ident, process.ExecuteReply(2345))
            server.socket.send(ident, process.StreamUpdate(2345, process.STDOUT, b'asdf\n'))
            server.socket.send(ident, process.StreamUpdate(2345, process.STDOUT))
            server.socket.send(ident, process.StreamUpdate(2345, process.STDERR))
            server.socket.send(ident, process.ExitUpdate(2345, 0))

        exit_a, exit_b = pipe(ctx)

        with HedgehogClient(ctx, 'inproc://controller') as client:
            def on_exit(client, pid, exit_code):
                self.assertEqual(pid, 2345)
                self.assertEqual(exit_code, 0)
                exit_b.signal()

            self.assertEqual(client.execute_process('echo', 'asdf', on_exit=on_exit), 2345)

        # exit_a.wait()

        thread.join()

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

    pass


if __name__ == '__main__':
    unittest.main()
