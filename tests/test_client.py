from typing import List

import pytest
from hedgehog.utils.test_utils import zmq_ctx

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
from hedgehog.server.hardware import HardwareAdapter
from hedgehog.server.hardware.mocked import MockedHardwareAdapter
from hedgehog.utils import coroutine
from hedgehog.utils.discovery.service_node import ServiceNode
from hedgehog.utils.zmq.pipe import pipe


# Pytest fixtures
zmq_ctx


def handler(adapter: HardwareAdapter=None) -> handlers.HandlerCallbackDict:
    if adapter is None:
        adapter = MockedHardwareAdapter()
    return handlers.to_dict(HardwareHandler(adapter), ProcessHandler(adapter))


class HedgehogServerDummy(object):
    def __init__(self, ctx, endpoint):
        self.socket = DealerRouterSocket(ctx, zmq.ROUTER, side=ServerSide)
        self.socket.bind(endpoint)

    def __call__(self, func):
        def target():
            try:
                func(self)

                ident, msgs = self.socket.recv_msgs()
                _msgs = []  # type: List[Message]
                _msgs.extend(motor.Action(port, motor.POWER, 0) for port in range(0, 4))
                _msgs.extend(servo.Action(port, False, 0) for port in range(0, 4))
                assert msgs == tuple(_msgs)
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


class TestHedgehogClient(object):
    def test_connect(self, zmq_ctx):
        @HedgehogServerDummy(zmq_ctx, 'inproc://controller')
        def thread(server):
            pass

        with HedgehogClient(zmq_ctx, 'inproc://controller') as client:
            pass

        thread.join()

    def test_single_client_thread(self, zmq_ctx):
        @HedgehogServerDummy(zmq_ctx, 'inproc://controller')
        def thread(server):
            ident, msg = server.socket.recv_msg()
            assert msg == analog.Request(0)
            server.socket.send_msg(ident, analog.Reply(0, 0))

        with HedgehogClient(zmq_ctx, 'inproc://controller') as client:
            assert client.get_analog(0) == 0

        thread.join()

    def test_multiple_client_threads(self, zmq_ctx):
        @HedgehogServerDummy(zmq_ctx, 'inproc://controller')
        def thread(server):
            ident1, msg = server.socket.recv_msg()
            assert msg == analog.Request(0)
            server.socket.send_msg(ident1, analog.Reply(0, 0))

            ident2, msg = server.socket.recv_msg()
            assert msg == analog.Request(0)
            server.socket.send_msg(ident2, analog.Reply(0, 0))

            assert ident1[0] == ident2[0]
            assert ident1[1] != ident2[1]

        with HedgehogClient(zmq_ctx, 'inproc://controller') as client:
            assert client.get_analog(0) == 0

            def spawned():
                assert client.get_analog(0) == 0

            client.spawn(spawned)

        thread.join()

    def test_unsupported(self, zmq_ctx):
        @HedgehogServerDummy(zmq_ctx, 'inproc://controller')
        def thread(server):
            ident, msg = server.socket.recv_msg()
            assert msg == analog.Request(0)
            server.socket.send_msg(ident, ack.Acknowledgement(ack.UNSUPPORTED_COMMAND))

        with HedgehogClient(zmq_ctx, 'inproc://controller') as client:
            with pytest.raises(errors.UnsupportedCommandError):
                client.get_analog(0)

        thread.join()

    def test_shutdown(self, zmq_ctx):
        @HedgehogServerDummy(zmq_ctx, 'inproc://controller')
        def thread(server):
            pass

        with HedgehogClient(zmq_ctx, 'inproc://controller') as client:
            client.shutdown()
            with pytest.raises(errors.FailedCommandError):
                client.get_analog(0)

        thread.join()


class TestClientConvenienceFunctions(object):
    def test_find_server(self, zmq_ctx):
        with ServiceNode(zmq_ctx, "Hedgehog Server") as node:
            SERVICE = 'hedgehog_server'

            node.join(SERVICE)
            node.add_service(SERVICE, 10789)

            server = find_server(zmq_ctx, SERVICE)
            port = list(server.services[SERVICE])[0].rsplit(':', 1)[1]

            assert port == "10789"

    @pytest.mark.skip
    def test_get_client(self, zmq_ctx):
        with HedgehogServer(zmq_ctx, 'inproc://controller', handler()):
            with get_client('inproc://controller', ctx=zmq_ctx) as client:
                assert client.get_analog(0) == 0

    @pytest.mark.skip
    def test_connect(self, zmq_ctx):
        with HedgehogServer(zmq_ctx, 'inproc://controller', handler()):
            with connect('inproc://controller', ctx=zmq_ctx, process_setup=False) as client:
                assert client.get_analog(0) == 0

    @unittest.skip
    def test_connect_with_emergency_shutdown(self, zmq_ctx):
        with HedgehogServer(zmq_ctx, 'inproc://controller', handler()):
            with connect('inproc://controller', emergency=0, ctx=zmq_ctx, process_setup=False) as client:
                assert client.get_analog(0) == 0


class command(object):
    def __init__(self, respond):
        self.respond = respond

    def request(self, request):
        return lambda _self, *args, **kwargs: (
            lambda client: request(_self, client, *args, **kwargs),
            lambda server: self.respond(_self, server, *args, **kwargs),
        )


class HedgehogAPITestCase(object):
    client_class = HedgehogClient

    def run_test(self, zmq_ctx, *requests):
        @HedgehogServerDummy(zmq_ctx, 'inproc://controller')
        def thread(server):
            for _, respond in requests:
                respond(server)

        with self.client_class(zmq_ctx, 'inproc://controller') as client:
            for request, _ in requests:
                request(client)

        thread.join()

    @command
    def io_action_input(self, server, port, pullup):
        ident, msg = server.socket.recv_msg()
        assert msg == io.Action(port, io.INPUT_PULLUP if pullup else io.INPUT_FLOATING)
        server.socket.send_msg(ident, ack.Acknowledgement())

    @command
    def io_command_request(self, server, port, flags):
        ident, msg = server.socket.recv_msg()
        assert msg == io.CommandRequest(port)
        server.socket.send_msg(ident, io.CommandReply(port, flags))

    @command
    def analog_request(self, server, port, value):
        ident, msg = server.socket.recv_msg()
        assert msg == analog.Request(port)
        server.socket.send_msg(ident, analog.Reply(port, value))

    @command
    def digital_request(self, server, port, value):
        ident, msg = server.socket.recv_msg()
        assert msg == digital.Request(port)
        server.socket.send_msg(ident, digital.Reply(port, value))

    @command
    def io_action_output(self, server, port, level):
        ident, msg = server.socket.recv_msg()
        assert msg == io.Action(port, io.OUTPUT_ON if level else io.OUTPUT_OFF)
        server.socket.send_msg(ident, ack.Acknowledgement())

    @command
    def motor_action(self, server, port, state, amount):
        ident, msg = server.socket.recv_msg()
        assert msg == motor.Action(port, state, amount)
        server.socket.send_msg(ident, ack.Acknowledgement())

    @command
    def motor_command_request(self, server, port, state, amount):
        ident, msg = server.socket.recv_msg()
        assert msg == motor.CommandRequest(port)
        server.socket.send_msg(ident, motor.CommandReply(port, state, amount))

    @command
    def motor_state_request(self, server, port, velocity, position):
        ident, msg = server.socket.recv_msg()
        assert msg == motor.StateRequest(port)
        server.socket.send_msg(ident, motor.StateReply(port, velocity, position))

    @command
    def motor_set_position_action(self, server, port, position):
        ident, msg = server.socket.recv_msg()
        assert msg == motor.SetPositionAction(port, position)
        server.socket.send_msg(ident, ack.Acknowledgement())

    @command
    def servo_action(self, server, port, active, position):
        ident, msg = server.socket.recv_msg()
        assert msg == servo.Action(port, active, position)
        server.socket.send_msg(ident, ack.Acknowledgement())

    @command
    def servo_command_request(self, server, port, active, position):
        ident, msg = server.socket.recv_msg()
        assert msg == servo.CommandRequest(port)
        server.socket.send_msg(ident, servo.CommandReply(port, active, position))

    @command
    def execute_process_echo_asdf(self, server, pid):
        ident, msg = server.socket.recv_msg()
        assert msg == process.ExecuteAction('echo', 'asdf')
        server.socket.send_msg(ident, process.ExecuteReply(pid))
        server.socket.send_msg(ident, process.StreamUpdate(pid, process.STDOUT, b'asdf\n'))
        server.socket.send_msg(ident, process.StreamUpdate(pid, process.STDOUT))
        server.socket.send_msg(ident, process.StreamUpdate(pid, process.STDERR))
        server.socket.send_msg(ident, process.ExitUpdate(pid, 0))

    @command
    def execute_process_cat(self, server, pid):
        ident, msg = server.socket.recv_msg()
        assert msg == process.ExecuteAction('cat')
        server.socket.send_msg(ident, process.ExecuteReply(pid))

        while True:
            ident, msg = server.socket.recv_msg()
            chunk = msg.chunk
            assert msg == process.StreamAction(pid, process.STDIN, chunk)
            server.socket.send_msg(ident, ack.Acknowledgement())

            server.socket.send_msg(ident, process.StreamUpdate(pid, process.STDOUT, chunk))

            if chunk == b'':
                break

        server.socket.send_msg(ident, process.StreamUpdate(pid, process.STDERR))
        server.socket.send_msg(ident, process.ExitUpdate(pid, 0))


class TestHedgehogClientAPI(HedgehogAPITestCase):
    @HedgehogAPITestCase.io_action_input.request
    def set_input_state(self, client, port, pullup):
        assert client.set_input_state(port, pullup) is None

    @HedgehogAPITestCase.io_command_request.request
    def get_io_config(self, client, port, flags):
        assert client.get_io_config(port) == flags

    @HedgehogAPITestCase.analog_request.request
    def get_analog(self, client, port, value):
        assert client.get_analog(port) == value

    @HedgehogAPITestCase.digital_request.request
    def get_digital(self, client, port, value):
        assert client.get_digital(port) == value

    @HedgehogAPITestCase.io_action_output.request
    def set_digital_output(self, client, port, level):
        assert client.set_digital_output(port, level) is None

    def test_ios(self, zmq_ctx):
        self.run_test(
            zmq_ctx,
            self.set_input_state(0, False),
            self.get_io_config(0, io.INPUT_FLOATING),
            self.get_analog(0, 0),
            self.get_digital(0, False),
            self.set_digital_output(0, False),
        )

    @HedgehogAPITestCase.motor_action.request
    def set_motor(self, client, port, state, amount):
        assert client.set_motor(port, state, amount) is None

    @HedgehogAPITestCase.motor_command_request.request
    def get_motor_command(self, client, port, state, amount):
        assert client.get_motor_command(port) == (state, amount)

    @HedgehogAPITestCase.motor_state_request.request
    def get_motor_state(self, client, port, velocity, position):
        assert client.get_motor_state(port) == (velocity, position)

    @HedgehogAPITestCase.motor_set_position_action.request
    def set_motor_position(self, client, port, position):
        assert client.set_motor_position(port, position) is None

    def test_motor(self, zmq_ctx):
        self.run_test(
            zmq_ctx,
            self.set_motor(0, motor.POWER, 100),
            self.get_motor_command(0, motor.POWER, 0),
            self.get_motor_state(0, 0, 0),
            self.set_motor_position(0, 0),
        )

    @HedgehogAPITestCase.servo_action.request
    def set_servo(self, client, port, active, position):
        assert client.set_servo(port, active, position) is None

    @HedgehogAPITestCase.servo_command_request.request
    def get_servo_command(self, client, port, active, position):
        assert client.get_servo_command(port) == (active, position)

    def test_servo(self, zmq_ctx):
        self.run_test(
            zmq_ctx,
            self.set_servo(0, False, 0),
            self.get_servo_command(0, False, None),
            self.get_servo_command(0, True, 0),
        )


class TestHedgehogClientProcessAPI(HedgehogAPITestCase):
    @HedgehogAPITestCase.execute_process_echo_asdf.request
    def execute_process_handle_nothing(self, client, pid):
        assert client.execute_process('echo', 'asdf') == pid

    @HedgehogAPITestCase.execute_process_echo_asdf.request
    def execute_process_handle_exit(self, client, pid):
        with zmq.Context() as ctx:
            exit_a, exit_b = pipe(ctx)

            @coroutine
            def on_exit():
                _pid, exit_code = yield
                assert _pid == pid
                assert exit_code == 0
                exit_b.signal()
                exit_b.close()
                yield

            assert client.execute_process('echo', 'asdf', on_exit=on_exit()) == pid

            exit_a.wait()
            exit_a.close()

    @HedgehogAPITestCase.execute_process_echo_asdf.request
    def execute_process_handle_stream(self, client, pid):
        with zmq.Context() as ctx:
            exit_a, exit_b = pipe(ctx)

            @coroutine
            def on_stdout():
                _pid, fileno, chunk = yield
                assert _pid == pid
                assert fileno == process.STDOUT
                assert chunk == b'asdf\n'

                _pid, fileno, chunk = yield
                assert _pid == pid
                assert fileno == process.STDOUT
                assert chunk == b''

                exit_b.signal()
                exit_b.close()
                yield

            assert client.execute_process('echo', 'asdf', on_stdout=on_stdout()) == pid

            exit_a.wait()
            exit_a.close()

    @HedgehogAPITestCase.execute_process_cat.request
    def execute_process_handle_input(self, client, pid):
        assert client.execute_process('cat') == pid
        assert client.send_process_data(pid, b'asdf\n') is None
        assert client.send_process_data(pid) is None

    def test_execute_process(self, zmq_ctx):
        self.run_test(
            zmq_ctx,
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
        assert client.analog(port).set_state(pullup) is None

    @HedgehogAPITestCase.io_command_request.request
    def analog_get_config(self, client, port, flags):
        assert client.analog(port).get_config() == flags

    @HedgehogAPITestCase.io_action_input.request
    def digital_set_state(self, client, port, pullup):
        assert client.digital(port).set_state(pullup) is None

    @HedgehogAPITestCase.io_command_request.request
    def digital_get_config(self, client, port, flags):
        assert client.digital(port).get_config() == flags

    @HedgehogAPITestCase.analog_request.request
    def analog_get(self, client, port, value):
        assert client.analog(port).get() == value

    @HedgehogAPITestCase.digital_request.request
    def digital_get(self, client, port, value):
        assert client.digital(port).get() == value

    @HedgehogAPITestCase.io_action_output.request
    def output_set(self, client, port, level):
        assert client.output(port).set(level) is None

    @HedgehogAPITestCase.io_command_request.request
    def output_get_config(self, client, port, flags):
        assert client.output(port).get_config() == flags

    def test_ios(self, zmq_ctx):
        self.run_test(
            zmq_ctx,
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
        assert client.motor(port).set(state, amount) is None

    @HedgehogAPITestCase.motor_command_request.request
    def motor_get_command(self, client, port, state, amount):
        assert client.motor(port).get_command() == (state, amount)

    @HedgehogAPITestCase.motor_state_request.request
    def motor_get_state(self, client, port, velocity, position):
        assert client.motor(port).get_state() == (velocity, position)

    @HedgehogAPITestCase.motor_set_position_action.request
    def motor_set_position(self, client, port, position):
        assert client.motor(port).set_position(position) is None

    def test_motor(self, zmq_ctx):
        self.run_test(
            zmq_ctx,
            self.motor_set(0, motor.POWER, 100),
            self.motor_get_command(0, 0, 0),
            self.motor_get_state(0, 0, 0),
            self.motor_set_position(0, 0),
        )

    @HedgehogAPITestCase.servo_action.request
    def servo_set(self, client, port, active, position):
        assert client.servo(port).set(active, position) is None

    @HedgehogAPITestCase.servo_command_request.request
    def servo_get_command(self, client, port, active, position):
        assert client.servo(port).get_command() == (active, position)

    def test_servo(self, zmq_ctx):
        self.run_test(
            zmq_ctx,
            self.servo_set(0, False, 0),
            self.servo_get_command(0, False, None),
            self.servo_get_command(0, True, 0),
        )


class TestComponentGetterProcessAPI(HedgehogAPITestCase):
    class HedgehogComponentGetterClient(HedgehogComponentGetterMixin, HedgehogClient):
        pass

    client_class = HedgehogComponentGetterClient

    @HedgehogAPITestCase.execute_process_echo_asdf.request
    def execute_process_handle_nothing(self, client, pid):
        assert client.process('echo', 'asdf').pid == pid

    @HedgehogAPITestCase.execute_process_echo_asdf.request
    def execute_process_handle_exit(self, client, pid):
        ctx = zmq.Context()
        exit_a, exit_b = pipe(ctx)

        @coroutine
        def on_exit():
            _pid, exit_code = yield
            assert _pid == pid
            assert exit_code == 0
            exit_b.signal()
            exit_b.close()
            yield

        assert client.process('echo', 'asdf', on_exit=on_exit()).pid == pid

        exit_a.wait()
        exit_a.close()

    @HedgehogAPITestCase.execute_process_echo_asdf.request
    def execute_process_handle_stream(self, client, pid):
        ctx = zmq.Context()
        exit_a, exit_b = pipe(ctx)

        @coroutine
        def on_stdout():
            _pid, fileno, chunk = yield
            assert _pid == pid
            assert fileno == process.STDOUT
            assert chunk == b'asdf\n'

            _pid, fileno, chunk = yield
            assert _pid == pid
            assert fileno == process.STDOUT
            assert chunk == b''

            exit_b.signal()
            exit_b.close()
            yield

        assert client.process('echo', 'asdf', on_stdout=on_stdout()).pid == pid

        exit_a.wait()
        exit_a.close()

    @HedgehogAPITestCase.execute_process_cat.request
    def execute_process_handle_input(self, client, pid):
        process = client.process('cat')
        assert process.pid == pid
        assert process.send_data(b'asdf\n') is None
        assert process.send_data() is None

    def test_execute_process(self, zmq_ctx):
        self.run_test(
            zmq_ctx,
            self.execute_process_handle_nothing(2345),
            self.execute_process_handle_exit(2346),
            self.execute_process_handle_stream(2347),
            self.execute_process_handle_input(2348),
        )
