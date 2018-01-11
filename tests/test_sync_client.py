import pytest
from hedgehog.utils.test_utils import zmq_ctx, zmq_aio_ctx

import time
import zmq.asyncio
from contextlib import contextmanager

from hedgehog.client.sync_client import HedgehogClient
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
from hedgehog.utils.event_loop import EventLoopThread


# Pytest fixtures
zmq_ctx, zmq_aio_ctx


def handler(adapter: HardwareAdapter=None) -> handlers.HandlerCallbackDict:
    if adapter is None:
        adapter = MockedHardwareAdapter()
    return handlers.to_dict(HardwareHandler(adapter), ProcessHandler(adapter))


# @contextmanager
# def connect_dummy(ctx: zmq.Context, dummy: Callable[[DealerRouterSocket], None], *args,
#                   endpoint: str='inproc://controller', client_class=HedgehogClient, **kwargs):
#     with DealerRouterSocket(ctx, zmq.ROUTER, side=ServerSide) as socket:
#         socket.bind(endpoint)
#
#         exception = None
#
#         def target():
#             try:
#                 dummy(socket, *args, **kwargs)
#
#                 ident, msgs = socket.recv_msgs()
#                 _msgs = []  # type: List[Message]
#                 _msgs.extend(motor.Action(port, motor.POWER, 0) for port in range(0, 4))
#                 _msgs.extend(servo.Action(port, False, 0) for port in range(0, 4))
#                 assert msgs == tuple(_msgs)
#                 socket.send_msgs(ident, [ack.Acknowledgement()] * 8)
#             except Exception as exc:
#                 nonlocal exception
#                 exception = exc
#
#         thread = threading.Thread(target=target, name=traceback.extract_stack(limit=2)[0].name)
#         thread.start()
#
#         try:
#             with client_class(ctx, endpoint) as client:
#                 yield client
#         finally:
#             thread.join()
#             if exception is not None:
#                 raise exception


def test_connect(zmq_aio_ctx: zmq.asyncio.Context):
    with EventLoopThread() as looper,\
            looper.context(HedgehogServer(zmq_aio_ctx, 'inproc://controller', handler())), \
            HedgehogClient(zmq_aio_ctx, endpoint='inproc://controller') as client:
        client.set_input_state(0, False)


def test_overlapping_contexts(zmq_aio_ctx: zmq.asyncio.Context):
    with EventLoopThread() as looper,\
            looper.context(HedgehogServer(zmq_aio_ctx, 'inproc://controller', handler())):
        with HedgehogClient(zmq_aio_ctx, endpoint='inproc://controller') as client:
            def do_something():
                time.sleep(0.2)
                assert client.get_analog(0) == 0

            thread = client.spawn(do_something)
            assert client.get_analog(0) == 0
            time.sleep(0.1)
        thread.join()


def test_inactive_context(zmq_aio_ctx: zmq.asyncio.Context):
    with EventLoopThread() as looper,\
            looper.context(HedgehogServer(zmq_aio_ctx, 'inproc://controller', handler())):
        client = HedgehogClient(zmq_aio_ctx, endpoint='inproc://controller')

        with pytest.raises(RuntimeError):
            client.get_analog(0)

        with client:
            assert client.get_analog(0) == 0

        with pytest.raises(RuntimeError):
            client.get_analog(0)


def test_shutdown_context(zmq_aio_ctx: zmq.asyncio.Context):
    with EventLoopThread() as looper,\
            looper.context(HedgehogServer(zmq_aio_ctx, 'inproc://controller', handler())), \
            HedgehogClient(zmq_aio_ctx, endpoint='inproc://controller') as client:
        def do_something():
            assert client.get_analog(0) == 0
            time.sleep(0.2)
            with pytest.raises(errors.HedgehogCommandError):
                assert client.get_analog(0) == 0

        thread = client.spawn(do_something, daemon=True)
        time.sleep(0.1)
        client.shutdown()
        with pytest.raises(errors.HedgehogCommandError):
            assert client.get_analog(0) == 0
    thread.join()


def test_daemon_context(zmq_aio_ctx: zmq.asyncio.Context):
    with EventLoopThread() as looper,\
            looper.context(HedgehogServer(zmq_aio_ctx, 'inproc://controller', handler())):
        with HedgehogClient(zmq_aio_ctx, endpoint='inproc://controller') as client:
            def do_something():
                assert client.get_analog(0) == 0
                time.sleep(0.2)
                with pytest.raises(errors.HedgehogCommandError):
                    assert client.get_analog(0) == 0

            thread = client.spawn(do_something, daemon=True)
            time.sleep(0.1)
        thread.join()


# class Commands(object):
#     @staticmethod
#     def io_action_input(server, port, pullup):
#         ident, msg = server.recv_msg()
#         assert msg == io.Action(port, io.INPUT_PULLUP if pullup else io.INPUT_FLOATING)
#         server.send_msg(ident, ack.Acknowledgement())
#
#     @staticmethod
#     def io_command_request(server, port, flags):
#         ident, msg = server.recv_msg()
#         assert msg == io.CommandRequest(port)
#         server.send_msg(ident, io.CommandReply(port, flags))
#
#     @staticmethod
#     def analog_request(server, port, value):
#         ident, msg = server.recv_msg()
#         assert msg == analog.Request(port)
#         server.send_msg(ident, analog.Reply(port, value))
#
#     @staticmethod
#     def digital_request(server, port, value):
#         ident, msg = server.recv_msg()
#         assert msg == digital.Request(port)
#         server.send_msg(ident, digital.Reply(port, value))
#
#     @staticmethod
#     def io_action_output(server, port, level):
#         ident, msg = server.recv_msg()
#         assert msg == io.Action(port, io.OUTPUT_ON if level else io.OUTPUT_OFF)
#         server.send_msg(ident, ack.Acknowledgement())
#
#     @staticmethod
#     def motor_action(server, port, state, amount):
#         ident, msg = server.recv_msg()
#         assert msg == motor.Action(port, state, amount)
#         server.send_msg(ident, ack.Acknowledgement())
#
#     @staticmethod
#     def motor_command_request(server, port, state, amount):
#         ident, msg = server.recv_msg()
#         assert msg == motor.CommandRequest(port)
#         server.send_msg(ident, motor.CommandReply(port, state, amount))
#
#     @staticmethod
#     def motor_state_request(server, port, velocity, position):
#         ident, msg = server.recv_msg()
#         assert msg == motor.StateRequest(port)
#         server.send_msg(ident, motor.StateReply(port, velocity, position))
#
#     @staticmethod
#     def motor_set_position_action(server, port, position):
#         ident, msg = server.recv_msg()
#         assert msg == motor.SetPositionAction(port, position)
#         server.send_msg(ident, ack.Acknowledgement())
#
#     @staticmethod
#     def servo_action(server, port, active, position):
#         ident, msg = server.recv_msg()
#         assert msg == servo.Action(port, active, position)
#         server.send_msg(ident, ack.Acknowledgement())
#
#     @staticmethod
#     def servo_command_request(server, port, active, position):
#         ident, msg = server.recv_msg()
#         assert msg == servo.CommandRequest(port)
#         server.send_msg(ident, servo.CommandReply(port, active, position))
#
#     @staticmethod
#     def execute_process_echo_asdf(server, pid):
#         ident, msg = server.recv_msg()
#         assert msg == process.ExecuteAction('echo', 'asdf')
#         server.send_msg(ident, process.ExecuteReply(pid))
#         server.send_msg(ident, process.StreamUpdate(pid, process.STDOUT, b'asdf\n'))
#         server.send_msg(ident, process.StreamUpdate(pid, process.STDOUT))
#         server.send_msg(ident, process.StreamUpdate(pid, process.STDERR))
#         server.send_msg(ident, process.ExitUpdate(pid, 0))
#
#     @staticmethod
#     def execute_process_cat(server, pid):
#         ident, msg = server.recv_msg()
#         assert msg == process.ExecuteAction('cat')
#         server.send_msg(ident, process.ExecuteReply(pid))
#
#         while True:
#             ident, msg = server.recv_msg()
#             chunk = msg.chunk
#             assert msg == process.StreamAction(pid, process.STDIN, chunk)
#             server.send_msg(ident, ack.Acknowledgement())
#
#             server.send_msg(ident, process.StreamUpdate(pid, process.STDOUT, chunk))
#
#             if chunk == b'':
#                 break
#
#         server.send_msg(ident, process.StreamUpdate(pid, process.STDERR))
#         server.send_msg(ident, process.ExitUpdate(pid, 0))
#
#
# class HedgehogAPITestCase(object):
#     client_class = HedgehogClient
#
#     @pytest.fixture
#     def connect(self, zmq_ctx, command):
#         @contextmanager
#         def do_connect(*args, **kwargs):
#             with connect_dummy(zmq_ctx, command, *args, client_class=self.client_class,
#                                **kwargs) as client:
#                 yield client
#
#         return do_connect
#
#
# class TestHedgehogClientAPI(HedgehogAPITestCase):
#     @pytest.mark.parametrize('command', [Commands.io_action_input])
#     def test_set_input_state(self, connect):
#         port, pullup = 0, False
#         with connect(port, pullup) as client:
#             assert client.set_input_state(port, pullup) is None
#
#     @pytest.mark.parametrize('command', [Commands.io_command_request])
#     def test_get_io_config(self, connect):
#         port, flags = 0, io.INPUT_FLOATING
#         with connect(port, flags) as client:
#             assert client.get_io_config(port) == flags
#
#     @pytest.mark.parametrize('command', [Commands.analog_request])
#     def test_get_analog(self, connect):
#         port, value = 0, 0
#         with connect(port, value) as client:
#             assert client.get_analog(port) == value
#
#     @pytest.mark.parametrize('command', [Commands.digital_request])
#     def test_get_digital(self, connect):
#         port, value = 0, False
#         with connect(port, value) as client:
#             assert client.get_digital(port) == value
#
#     @pytest.mark.parametrize('command', [Commands.io_action_output])
#     def test_set_digital_output(self, connect):
#         port, level = 0, False
#         with connect(port, level) as client:
#             assert client.set_digital_output(port, level) is None
#
#     @pytest.mark.parametrize('command', [Commands.motor_action])
#     def test_set_motor(self, connect):
#         port, state, amount = 0, motor.POWER, 100
#         with connect(port, state, amount) as client:
#             assert client.set_motor(port, state, amount) is None
#
#     @pytest.mark.parametrize('command', [Commands.motor_command_request])
#     def test_get_motor_command(self, connect):
#         port, state, amount = 0, motor.POWER, 0
#         with connect(port, state, amount) as client:
#             assert client.get_motor_command(port) == (state, amount)
#
#     @pytest.mark.parametrize('command', [Commands.motor_state_request])
#     def test_get_motor_state(self, connect):
#         port, velocity, position = 0, 0, 0
#         with connect(port, velocity, position) as client:
#             assert client.get_motor_state(port) == (velocity, position)
#
#     @pytest.mark.parametrize('command', [Commands.motor_set_position_action])
#     def test_set_motor_position(self, connect):
#         port, position = 0, 0
#         with connect(port, position) as client:
#             assert client.set_motor_position(port, position) is None
#
#     @pytest.mark.parametrize('command', [Commands.servo_action])
#     def test_set_servo(self, connect):
#         port, active, position = 0, False, 0
#         with connect(port, active, position) as client:
#             assert client.set_servo(port, active, position) is None
#
#     @pytest.mark.parametrize('command', [Commands.servo_command_request])
#     def test_get_servo_command(self, connect):
#         port, active, position = 0, False, None
#         with connect(port, active, position) as client:
#             assert client.get_servo_command(port) == (active, position)
#
#         port, active, position = 0, True, 0
#         with connect(port, active, position) as client:
#             assert client.get_servo_command(port) == (active, position)
#
#
# class TestHedgehogClientProcessAPI(HedgehogAPITestCase):
#     @pytest.mark.parametrize('command', [Commands.execute_process_echo_asdf])
#     def test_execute_process_handle_nothing(self, connect):
#         pid = 2345
#         with connect(pid) as client:
#             assert client.execute_process('echo', 'asdf') == pid
#
#     @pytest.mark.parametrize('command', [Commands.execute_process_echo_asdf])
#     def test_execute_process_handle_exit(self, zmq_ctx, connect):
#         pid = 2346
#         with connect(pid) as client:
#             exit_a, exit_b = pipe(zmq_ctx)
#             with exit_a, exit_b:
#                 @coroutine
#                 def on_exit():
#                     _pid, exit_code = yield
#                     assert _pid == pid
#                     assert exit_code == 0
#                     exit_b.signal()
#                     yield
#
#                 assert client.execute_process('echo', 'asdf', on_exit=on_exit()) == pid
#
#                 exit_a.wait()
#
#     @pytest.mark.parametrize('command', [Commands.execute_process_echo_asdf])
#     def test_execute_process_handle_stream(self, zmq_ctx, connect):
#         pid = 2347
#         with connect(pid) as client:
#             exit_a, exit_b = pipe(zmq_ctx)
#             with exit_a, exit_b:
#                 @coroutine
#                 def on_stdout():
#                     _pid, fileno, chunk = yield
#                     assert _pid == pid
#                     assert fileno == process.STDOUT
#                     assert chunk == b'asdf\n'
#
#                     _pid, fileno, chunk = yield
#                     assert _pid == pid
#                     assert fileno == process.STDOUT
#                     assert chunk == b''
#
#                     exit_b.signal()
#                     yield
#
#                 assert client.execute_process('echo', 'asdf', on_stdout=on_stdout()) == pid
#
#                 exit_a.wait()
#
#     @pytest.mark.parametrize('command', [Commands.execute_process_cat])
#     def test_execute_process_handle_input(self, connect):
#         pid = 2348
#         with connect(pid) as client:
#             assert client.execute_process('cat') == pid
#             assert client.send_process_data(pid, b'asdf\n') is None
#             assert client.send_process_data(pid) is None
#
#
# class TestComponentGetterAPI(HedgehogAPITestCase):
#     class HedgehogComponentGetterClient(HedgehogComponentGetterMixin, HedgehogClient):
#         pass
#
#     client_class = HedgehogComponentGetterClient
#
#     @pytest.mark.parametrize('command', [Commands.io_action_input])
#     def test_test_analog_set_state(self, connect):
#         port, pullup = 0, False
#         with connect(port, pullup) as client:
#             assert client.analog(port).set_state(pullup) is None
#
#     @pytest.mark.parametrize('command', [Commands.io_command_request])
#     def test_analog_get_config(self, connect):
#         port, flags = 0, io.INPUT_FLOATING
#         with connect(port, flags) as client:
#             assert client.analog(port).get_config() == flags
#
#     @pytest.mark.parametrize('command', [Commands.io_action_input])
#     def test_test_digital_set_state(self, connect):
#         port, pullup = 0, False
#         with connect(port, pullup) as client:
#             assert client.digital(port).set_state(pullup) is None
#
#     @pytest.mark.parametrize('command', [Commands.io_command_request])
#     def test_digital_get_config(self, connect):
#         port, flags = 0, io.INPUT_FLOATING
#         with connect(port, flags) as client:
#             assert client.digital(port).get_config() == flags
#
#     @pytest.mark.parametrize('command', [Commands.analog_request])
#     def test_analog_get(self, connect):
#         port, value = 0, 0
#         with connect(port, value) as client:
#             assert client.analog(port).get() == value
#
#     @pytest.mark.parametrize('command', [Commands.digital_request])
#     def test_digital_get(self, connect):
#         port, value = 0, False
#         with connect(port, value) as client:
#             assert client.digital(port).get() == value
#
#     @pytest.mark.parametrize('command', [Commands.io_action_output])
#     def test_output_set(self, connect):
#         port, level = 0, False
#         with connect(port, level) as client:
#             assert client.output(port).set(level) is None
#
#     @pytest.mark.parametrize('command', [Commands.io_command_request])
#     def test_output_get_config(self, connect):
#         port, flags = 0, io.OUTPUT_OFF
#         with connect(port, flags) as client:
#             assert client.output(port).get_config() == flags
#
#     @pytest.mark.parametrize('command', [Commands.motor_action])
#     def test_motor_set(self, connect):
#         port, state, amount =0, motor.POWER, 100
#         with connect(port, state, amount) as client:
#             assert client.motor(port).set(state, amount) is None
#
#     @pytest.mark.parametrize('command', [Commands.motor_command_request])
#     def test_motor_get_command(self, connect):
#         port, state, amount = 0, 0, 0
#         with connect(port, state, amount) as client:
#             assert client.motor(port).get_command() == (state, amount)
#
#     @pytest.mark.parametrize('command', [Commands.motor_state_request])
#     def test_motor_get_state(self, connect):
#         port, velocity, position = 0, 0, 0
#         with connect(port, velocity, position) as client:
#             assert client.motor(port).get_state() == (velocity, position)
#
#     @pytest.mark.parametrize('command', [Commands.motor_set_position_action])
#     def test_motor_set_position(self, connect):
#         port, position = 0, 0
#         with connect(port, position) as client:
#             assert client.motor(port).set_position(position) is None
#
#     @pytest.mark.parametrize('command', [Commands.servo_action])
#     def test_servo_set(self, connect):
#         port, active, position = 0, False, 0
#         with connect(port, active, position) as client:
#             assert client.servo(port).set(active, position) is None
#
#     @pytest.mark.parametrize('command', [Commands.servo_command_request, ])
#     def test_servo_get_command(self, connect):
#         port, active, position = 0, False, None
#         with connect(port, active, position) as client:
#             assert client.servo(port).get_command() == (active, position)
#
#         port, active, position = 0, True, 0
#         with connect(port, active, position) as client:
#             assert client.servo(port).get_command() == (active, position)
#
#
# class TestComponentGetterProcessAPI(HedgehogAPITestCase):
#     class HedgehogComponentGetterClient(HedgehogComponentGetterMixin, HedgehogClient):
#         pass
#
#     client_class = HedgehogComponentGetterClient
#
#     @pytest.mark.parametrize('command', [Commands.execute_process_echo_asdf])
#     def test_execute_process_handle_nothing(self, connect):
#         pid = 2345
#         with connect(pid) as client:
#             assert client.process('echo', 'asdf').pid == pid
#
#     @pytest.mark.parametrize('command', [Commands.execute_process_echo_asdf])
#     def test_execute_process_handle_exit(self, zmq_ctx, connect):
#         pid = 2346
#         with connect(pid) as client:
#             exit_a, exit_b = pipe(zmq_ctx)
#             with exit_a, exit_b:
#                 @coroutine
#                 def on_exit():
#                     _pid, exit_code = yield
#                     assert _pid == pid
#                     assert exit_code == 0
#                     exit_b.signal()
#                     yield
#
#                 assert client.process('echo', 'asdf', on_exit=on_exit()).pid == pid
#
#                 exit_a.wait()
#
#     @pytest.mark.parametrize('command', [Commands.execute_process_echo_asdf])
#     def test_execute_process_handle_stream(self, zmq_ctx, connect):
#         pid = 2347
#         with connect(pid) as client:
#             exit_a, exit_b = pipe(zmq_ctx)
#             with exit_a, exit_b:
#                 @coroutine
#                 def on_stdout():
#                     _pid, fileno, chunk = yield
#                     assert _pid == pid
#                     assert fileno == process.STDOUT
#                     assert chunk == b'asdf\n'
#
#                     _pid, fileno, chunk = yield
#                     assert _pid == pid
#                     assert fileno == process.STDOUT
#                     assert chunk == b''
#
#                     exit_b.signal()
#                     yield
#
#                 assert client.process('echo', 'asdf', on_stdout=on_stdout()).pid == pid
#
#                 exit_a.wait()
#
#     @pytest.mark.parametrize('command', [Commands.execute_process_cat])
#     def test_execute_process_handle_input(self, connect):
#         pid = 2348
#         with connect(pid) as client:
#             process = client.process('cat')
#             assert process.pid == pid
#             assert process.send_data(b'asdf\n') is None
#             assert process.send_data() is None
