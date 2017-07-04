from typing import List

import threading
import traceback
import unittest

import zmq
from hedgehog.client import HedgehogClient, find_server, get_client, connect
from hedgehog.client.components import HedgehogComponentGetterMixin
from hedgehog.protocol import errors, ServerSide
from hedgehog.protocol.messages import Message, ack, analog, digital, io, motor, servo, process
from hedgehog.protocol.sockets import DealerRouterSocket
from hedgehog.server import handlers, HedgehogServer
from hedgehog.server.handlers.hardware import HardwareHandler
from hedgehog.server.handlers.process import ProcessHandler
from hedgehog.server.hardware.simulated import SimulatedHardwareAdapter
from hedgehog.utils import coroutine
from hedgehog.utils.discovery.service_node import ServiceNode
from hedgehog.utils.zmq.pipe import pipe


def handler():
    adapter = SimulatedHardwareAdapter()
    return handlers.to_dict(HardwareHandler(adapter), ProcessHandler(adapter))


class HedgehogServerDummy(object):
    def __init__(self, testcase, ctx, endpoint):
        self.socket = DealerRouterSocket(ctx, zmq.ROUTER, side=ServerSide)
        self.socket.bind(endpoint)
        self.testcase = testcase

    def __call__(self, func):
        def target():
            try:
                func(self)

                ident, msgs = self.socket.recv_msgs()
                _msgs = []  # type: List[Message]
                _msgs.extend(motor.Action(port, motor.POWER, 0) for port in range(0, 4))
                _msgs.extend(servo.Action(port, False, 0) for port in range(0, 4))
                self.testcase.assertEqual(msgs, tuple(_msgs))
                self.socket.send_msgs(ident, [ack.Acknowledgement()] * 8)

                func.exc = None
            except Exception as exc:
                traceback.print_exc()
                func.exc = exc
            finally:
                self.socket.close()

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
        with zmq.Context() as ctx:
            @HedgehogServerDummy(self, ctx, 'inproc://controller')
            def thread(server):
                pass

            with HedgehogClient(ctx, 'inproc://controller') as client:
                pass

            thread.join()

    def test_single_client_thread(self):
        with zmq.Context() as ctx:
            @HedgehogServerDummy(self, ctx, 'inproc://controller')
            def thread(server):
                ident, msg = server.socket.recv_msg()
                self.assertEqual(msg, analog.Request(0))
                server.socket.send_msg(ident, analog.Reply(0, 0))

            with HedgehogClient(ctx, 'inproc://controller') as client:
                self.assertEqual(client.get_analog(0), 0)

            thread.join()

    def test_multiple_client_threads(self):
        with zmq.Context() as ctx:
            @HedgehogServerDummy(self, ctx, 'inproc://controller')
            def thread(server):
                ident1, msg = server.socket.recv_msg()
                self.assertEqual(msg, analog.Request(0))
                server.socket.send_msg(ident1, analog.Reply(0, 0))

                ident2, msg = server.socket.recv_msg()
                self.assertEqual(msg, analog.Request(0))
                server.socket.send_msg(ident2, analog.Reply(0, 0))

                self.assertEqual(ident1[0], ident2[0])
                self.assertNotEqual(ident1[1], ident2[1])

            with HedgehogClient(ctx, 'inproc://controller') as client:
                self.assertEqual(client.get_analog(0), 0)

                def spawned():
                    self.assertEqual(client.get_analog(0), 0)

                client.spawn(spawned)

            thread.join()

    def test_unsupported(self):
        with zmq.Context() as ctx:
            @HedgehogServerDummy(self, ctx, 'inproc://controller')
            def thread(server):
                ident, msg = server.socket.recv_msg()
                self.assertEqual(msg, analog.Request(0))
                server.socket.send_msg(ident, ack.Acknowledgement(ack.UNSUPPORTED_COMMAND))

            with HedgehogClient(ctx, 'inproc://controller') as client:
                with self.assertRaises(errors.UnsupportedCommandError):
                    client.get_analog(0)

            thread.join()

    def test_shutdown(self):
        with zmq.Context() as ctx:
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
        with zmq.Context() as ctx:
            with ServiceNode(ctx, "Hedgehog Server") as node:
                SERVICE = 'hedgehog_server'

                node.join(SERVICE)
                node.add_service(SERVICE, 10789)

                server = find_server(ctx, SERVICE)
                port = list(server.services[SERVICE])[0].rsplit(':', 1)[1]

                self.assertEqual(port, "10789")

    def test_get_client(self):
        with zmq.Context() as ctx:
            with HedgehogServer(ctx, 'inproc://controller', handler()):
                with get_client('inproc://controller', ctx=ctx) as client:
                    self.assertEqual(client.get_analog(0), 0)

    def test_connect(self):
        with zmq.Context() as ctx:
            with HedgehogServer(ctx, 'inproc://controller', handler()):
                with connect('inproc://controller', ctx=ctx) as client:
                    self.assertEqual(client.get_analog(0), 0)

    def test_connect_with_emergency_shutdown(self):
        with zmq.Context() as ctx:
            with HedgehogServer(ctx, 'inproc://controller', handler()):
                with connect('inproc://controller', emergency=0, ctx=ctx) as client:
                    self.assertEqual(client.get_analog(0), 0)


class command(object):
    def __init__(self, respond):
        self.respond = respond

    def request(self, request):
        return lambda _self, *args, **kwargs: (
            lambda client: request(_self, client, *args, **kwargs),
            lambda server: self.respond(_self, server, *args, **kwargs),
        )


class HedgehogAPITestCase(unittest.TestCase):
    client_class = HedgehogClient

    def run_test(self, *requests):
        with zmq.Context() as ctx:
            @HedgehogServerDummy(self, ctx, 'inproc://controller')
            def thread(server):
                for _, respond in requests:
                    respond(server)

            with self.client_class(ctx, 'inproc://controller') as client:
                for request, _ in requests:
                    request(client)

            thread.join()

    @command
    def io_action_input(self, server, port, pullup):
        ident, msg = server.socket.recv_msg()
        self.assertEqual(msg, io.Action(port, io.INPUT_PULLUP if pullup else io.INPUT_FLOATING))
        server.socket.send_msg(ident, ack.Acknowledgement())

    @command
    def io_command_request(self, server, port, flags):
        ident, msg = server.socket.recv_msg()
        self.assertEqual(msg, io.CommandRequest(port))
        server.socket.send_msg(ident, io.CommandReply(port, flags))

    @command
    def analog_request(self, server, port, value):
        ident, msg = server.socket.recv_msg()
        self.assertEqual(msg, analog.Request(port))
        server.socket.send_msg(ident, analog.Reply(port, value))

    @command
    def digital_request(self, server, port, value):
        ident, msg = server.socket.recv_msg()
        self.assertEqual(msg, digital.Request(port))
        server.socket.send_msg(ident, digital.Reply(port, value))

    @command
    def io_action_output(self, server, port, level):
        ident, msg = server.socket.recv_msg()
        self.assertEqual(msg, io.Action(port, io.OUTPUT_ON if level else io.OUTPUT_OFF))
        server.socket.send_msg(ident, ack.Acknowledgement())

    @command
    def motor_action(self, server, port, state, amount):
        ident, msg = server.socket.recv_msg()
        self.assertEqual(msg, motor.Action(port, state, amount))
        server.socket.send_msg(ident, ack.Acknowledgement())

    @command
    def motor_command_request(self, server, port, state, amount):
        ident, msg = server.socket.recv_msg()
        self.assertEqual(msg, motor.CommandRequest(port))
        server.socket.send_msg(ident, motor.CommandReply(port, state, amount))

    @command
    def motor_state_request(self, server, port, velocity, position):
        ident, msg = server.socket.recv_msg()
        self.assertEqual(msg, motor.StateRequest(port))
        server.socket.send_msg(ident, motor.StateReply(port, velocity, position))

    @command
    def motor_set_position_action(self, server, port, position):
        ident, msg = server.socket.recv_msg()
        self.assertEqual(msg, motor.SetPositionAction(port, position))
        server.socket.send_msg(ident, ack.Acknowledgement())

    @command
    def servo_action(self, server, port, active, position):
        ident, msg = server.socket.recv_msg()
        self.assertEqual(msg, servo.Action(port, active, position))
        server.socket.send_msg(ident, ack.Acknowledgement())

    @command
    def servo_command_request(self, server, port, active, position):
        ident, msg = server.socket.recv_msg()
        self.assertEqual(msg, servo.CommandRequest(port))
        server.socket.send_msg(ident, servo.CommandReply(port, active, position))

    @command
    def execute_process_echo_asdf(self, server, pid):
        ident, msg = server.socket.recv_msg()
        self.assertEqual(msg, process.ExecuteAction('echo', 'asdf'))
        server.socket.send_msg(ident, process.ExecuteReply(pid))
        server.socket.send_msg(ident, process.StreamUpdate(pid, process.STDOUT, b'asdf\n'))
        server.socket.send_msg(ident, process.StreamUpdate(pid, process.STDOUT))
        server.socket.send_msg(ident, process.StreamUpdate(pid, process.STDERR))
        server.socket.send_msg(ident, process.ExitUpdate(pid, 0))

    @command
    def execute_process_cat(self, server, pid):
        ident, msg = server.socket.recv_msg()
        self.assertEqual(msg, process.ExecuteAction('cat'))
        server.socket.send_msg(ident, process.ExecuteReply(pid))

        while True:
            ident, msg = server.socket.recv_msg()
            chunk = msg.chunk
            self.assertEqual(msg, process.StreamAction(pid, process.STDIN, chunk))
            server.socket.send_msg(ident, ack.Acknowledgement())

            server.socket.send_msg(ident, process.StreamUpdate(pid, process.STDOUT, chunk))

            if chunk == b'':
                break

        server.socket.send_msg(ident, process.StreamUpdate(pid, process.STDERR))
        server.socket.send_msg(ident, process.ExitUpdate(pid, 0))


class TestHedgehogClientAPI(HedgehogAPITestCase):
    @HedgehogAPITestCase.io_action_input.request
    def set_input_state(self, client, port, pullup):
        self.assertEqual(client.set_input_state(port, pullup), None)

    @HedgehogAPITestCase.io_command_request.request
    def get_io_config(self, client, port, flags):
        self.assertEqual(client.get_io_config(port), flags)

    @HedgehogAPITestCase.analog_request.request
    def get_analog(self, client, port, value):
        self.assertEqual(client.get_analog(port), value)

    @HedgehogAPITestCase.digital_request.request
    def get_digital(self, client, port, value):
        self.assertEqual(client.get_digital(port), value)

    @HedgehogAPITestCase.io_action_output.request
    def set_digital_output(self, client, port, level):
        self.assertEqual(client.set_digital_output(port, level), None)

    def test_ios(self):
        self.run_test(
            self.set_input_state(0, False),
            self.get_io_config(0, io.INPUT_FLOATING),
            self.get_analog(0, 0),
            self.get_digital(0, False),
            self.set_digital_output(0, False),
        )

    @HedgehogAPITestCase.motor_action.request
    def set_motor(self, client, port, state, amount):
        self.assertEqual(client.set_motor(port, state, amount), None)

    @HedgehogAPITestCase.motor_command_request.request
    def get_motor_command(self, client, port, state, amount):
        self.assertEqual(client.get_motor_command(port), (state, amount))

    @HedgehogAPITestCase.motor_state_request.request
    def get_motor_state(self, client, port, velocity, position):
        self.assertEqual(client.get_motor_state(port), (velocity, position))

    @HedgehogAPITestCase.motor_set_position_action.request
    def set_motor_position(self, client, port, position):
        self.assertEqual(client.set_motor_position(port, position), None)

    def test_motor(self):
        self.run_test(
            self.set_motor(0, motor.POWER, 100),
            self.get_motor_command(0, motor.POWER, 0),
            self.get_motor_state(0, 0, 0),
            self.set_motor_position(0, 0),
        )

    @HedgehogAPITestCase.servo_action.request
    def set_servo(self, client, port, active, position):
        self.assertEqual(client.set_servo(port, active, position), None)

    @HedgehogAPITestCase.servo_command_request.request
    def get_servo_command(self, client, port, active, position):
        self.assertEqual(client.get_servo_command(port), (active, position))

    def test_servo(self):
        self.run_test(
            self.set_servo(0, False, 0),
            self.get_servo_command(0, False, 0),
        )


class TestHedgehogClientProcessAPI(HedgehogAPITestCase):
    @HedgehogAPITestCase.execute_process_echo_asdf.request
    def execute_process_handle_nothing(self, client, pid):
        self.assertEqual(client.execute_process('echo', 'asdf'), pid)

    @HedgehogAPITestCase.execute_process_echo_asdf.request
    def execute_process_handle_exit(self, client, pid):
        with zmq.Context() as ctx:
            exit_a, exit_b = pipe(ctx)

            @coroutine
            def on_exit():
                _pid, exit_code = yield
                self.assertEqual(_pid, pid)
                self.assertEqual(exit_code, 0)
                exit_b.signal()
                exit_b.close()
                yield

            self.assertEqual(client.execute_process('echo', 'asdf', on_exit=on_exit()), pid)

            exit_a.wait()
            exit_a.close()

    @HedgehogAPITestCase.execute_process_echo_asdf.request
    def execute_process_handle_stream(self, client, pid):
        with zmq.Context() as ctx:
            exit_a, exit_b = pipe(ctx)

            @coroutine
            def on_stdout():
                _pid, fileno, chunk = yield
                self.assertEqual(_pid, pid)
                self.assertEqual(fileno, process.STDOUT)
                self.assertEqual(chunk, b'asdf\n')

                _pid, fileno, chunk = yield
                self.assertEqual(_pid, pid)
                self.assertEqual(fileno, process.STDOUT)
                self.assertEqual(chunk, b'')

                exit_b.signal()
                exit_b.close()
                yield

            self.assertEqual(client.execute_process('echo', 'asdf', on_stdout=on_stdout()), pid)

            exit_a.wait()
            exit_a.close()

    @HedgehogAPITestCase.execute_process_cat.request
    def execute_process_handle_input(self, client, pid):
        self.assertEqual(client.execute_process('cat'), pid)
        self.assertEqual(client.send_process_data(pid, b'asdf\n'), None)
        self.assertEqual(client.send_process_data(pid), None)

    def test_execute_process(self):
        self.run_test(
            self.execute_process_handle_nothing(2345),
            self.execute_process_handle_exit(2346),
            self.execute_process_handle_stream(2347),
            self.execute_process_handle_input(2348),
        )


class TestComponentGetterAPI(HedgehogAPITestCase):
    class HedgehogComponentGetterClient(HedgehogComponentGetterMixin, HedgehogClient):
        pass

    client_class = HedgehogComponentGetterClient

    @HedgehogAPITestCase.io_action_input.request
    def analog_set_state(self, client, port, pullup):
        self.assertEqual(client.analog(port).set_state(pullup), None)

    @HedgehogAPITestCase.io_command_request.request
    def analog_get_config(self, client, port, flags):
        self.assertEqual(client.analog(port).get_config(), flags)

    @HedgehogAPITestCase.io_action_input.request
    def digital_set_state(self, client, port, pullup):
        self.assertEqual(client.digital(port).set_state(pullup), None)

    @HedgehogAPITestCase.io_command_request.request
    def digital_get_config(self, client, port, flags):
        self.assertEqual(client.digital(port).get_config(), flags)

    @HedgehogAPITestCase.analog_request.request
    def analog_get(self, client, port, value):
        self.assertEqual(client.analog(port).get(), value)

    @HedgehogAPITestCase.digital_request.request
    def digital_get(self, client, port, value):
        self.assertEqual(client.digital(port).get(), value)

    @HedgehogAPITestCase.io_action_output.request
    def output_set(self, client, port, level):
        self.assertEqual(client.output(port).set(level), None)

    @HedgehogAPITestCase.io_command_request.request
    def output_get_config(self, client, port, flags):
        self.assertEqual(client.output(port).get_config(), flags)

    def test_ios(self):
        self.run_test(
            self.analog_set_state(0, False),
            self.analog_get_config(0, io.INPUT_FLOATING),
            self.digital_set_state(0, False),
            self.digital_get_config(0, io.INPUT_FLOATING),
            self.analog_get(0, 0),
            self.digital_get(0, False),
            self.output_set(0, False),
            self.output_get_config(0, io.OUTPUT_OFF),
        )

    @HedgehogAPITestCase.motor_action.request
    def motor_set(self, client, port, state, amount):
        self.assertEqual(client.motor(port).set(state, amount), None)

    @HedgehogAPITestCase.motor_command_request.request
    def motor_get_command(self, client, port, state, amount):
        self.assertEqual(client.motor(port).get_command(), (state, amount))

    @HedgehogAPITestCase.motor_state_request.request
    def motor_get_state(self, client, port, velocity, position):
        self.assertEqual(client.motor(port).get_state(), (velocity, position))

    @HedgehogAPITestCase.motor_set_position_action.request
    def motor_set_position(self, client, port, position):
        self.assertEqual(client.motor(port).set_position(position), None)

    def test_motor(self):
        self.run_test(
            self.motor_set(0, motor.POWER, 100),
            self.motor_get_command(0, 0, 0),
            self.motor_get_state(0, 0, 0),
            self.motor_set_position(0, 0),
        )

    @HedgehogAPITestCase.servo_action.request
    def servo_set(self, client, port, active, position):
        self.assertEqual(client.servo(port).set(active, position), None)

    @HedgehogAPITestCase.servo_command_request.request
    def servo_get_command(self, client, port, active, position):
        self.assertEqual(client.servo(port).get_command(), (active, position))

    def test_servo(self):
        self.run_test(
            self.servo_set(0, False, 0),
            self.servo_get_command(0, False, 0),
        )


class TestComponentGetterProcessAPI(HedgehogAPITestCase):
    class HedgehogComponentGetterClient(HedgehogComponentGetterMixin, HedgehogClient):
        pass

    client_class = HedgehogComponentGetterClient

    @HedgehogAPITestCase.execute_process_echo_asdf.request
    def execute_process_handle_nothing(self, client, pid):
        self.assertEqual(client.process('echo', 'asdf').pid, pid)

    @HedgehogAPITestCase.execute_process_echo_asdf.request
    def execute_process_handle_exit(self, client, pid):
        ctx = zmq.Context()
        exit_a, exit_b = pipe(ctx)

        @coroutine
        def on_exit():
            _pid, exit_code = yield
            self.assertEqual(_pid, pid)
            self.assertEqual(exit_code, 0)
            exit_b.signal()
            exit_b.close()
            yield

        self.assertEqual(client.process('echo', 'asdf', on_exit=on_exit()).pid, pid)

        exit_a.wait()
        exit_a.close()

    @HedgehogAPITestCase.execute_process_echo_asdf.request
    def execute_process_handle_stream(self, client, pid):
        ctx = zmq.Context()
        exit_a, exit_b = pipe(ctx)

        @coroutine
        def on_stdout():
            _pid, fileno, chunk = yield
            self.assertEqual(_pid, pid)
            self.assertEqual(fileno, process.STDOUT)
            self.assertEqual(chunk, b'asdf\n')

            _pid, fileno, chunk = yield
            self.assertEqual(_pid, pid)
            self.assertEqual(fileno, process.STDOUT)
            self.assertEqual(chunk, b'')

            exit_b.signal()
            exit_b.close()
            yield

        self.assertEqual(client.process('echo', 'asdf', on_stdout=on_stdout()).pid, pid)

        exit_a.wait()
        exit_a.close()

    @HedgehogAPITestCase.execute_process_cat.request
    def execute_process_handle_input(self, client, pid):
        process = client.process('cat')
        self.assertEqual(process.pid, pid)
        self.assertEqual(process.send_data(b'asdf\n'), None)
        self.assertEqual(process.send_data(), None)

    def test_execute_process(self):
        self.run_test(
            self.execute_process_handle_nothing(2345),
            self.execute_process_handle_exit(2346),
            self.execute_process_handle_stream(2347),
            self.execute_process_handle_input(2348),
        )


if __name__ == '__main__':
    unittest.main()
