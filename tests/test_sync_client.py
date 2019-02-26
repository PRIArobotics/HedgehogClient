from typing import Awaitable, Callable

import pytest
from hedgehog.utils.test_utils import zmq_aio_ctx
from hedgehog.client.test_utils import start_dummy, start_dummy_sync, Commands

import time
import zmq.asyncio
from contextlib import contextmanager

from concurrent_utils.pipe import PipeEnd
from hedgehog.client.sync_client import HedgehogClient, connect
from hedgehog.protocol import errors
from hedgehog.protocol.messages import io, motor, process
from hedgehog.protocol.zmq import DealerRouterSocket


# Pytest fixtures
zmq_aio_ctx, start_dummy, start_dummy_sync


# additional fixtures

@pytest.fixture
def connect_client(zmq_aio_ctx: zmq.asyncio.Context):
    @contextmanager
    def do_connect(endpoint, client_class=HedgehogClient):
        with client_class(zmq_aio_ctx, endpoint) as client:
            yield client

    return do_connect


@pytest.fixture
def connect_dummy(start_dummy_sync, connect_client):
    @contextmanager
    def do_connect(server_coro: Callable[[DealerRouterSocket], Awaitable[None]], *args,
                   endpoint: str='inproc://controller', client_class=HedgehogClient, **kwargs):
        with start_dummy_sync(server_coro, *args, endpoint=endpoint, **kwargs) as dummy, \
                connect_client(dummy, client_class=client_class) as client:
            yield client

    return do_connect


# tests

def test_multipart_commands(connect_dummy):
    from hedgehog.protocol.messages import analog, digital

    port_a, value_a, port_d, value_d = 0, 0, 0, True
    with connect_dummy(Commands.multipart_analog_digital_requests, port_a, value_a, port_d, value_d) as client:
        client.commands(
            analog.Request(port_a),
            digital.Request(port_d),
        )

    port_a, value_a, port_d, value_d = 0, 0, 16, True
    with connect_dummy(Commands.multipart_analog_digital_requests, port_a, value_a, port_d, value_d) as client:
        with pytest.raises(errors.FailedCommandError):
            client.commands(
                analog.Request(port_a),
                digital.Request(port_d),
            )


def test_command(connect_dummy):
    port, pullup = 0, False
    with connect_dummy(Commands.io_action_input, port, pullup) as client:
        client.set_input_state(0, False)


def test_overlapping_contexts(start_dummy_sync, connect_client):
    port_a, value_a, port_d, value_d = 0, 0, 0, True
    with start_dummy_sync(Commands.concurrent_analog_digital_requests, port_a, value_a, port_d, value_d) as server:
        with connect_client(server) as client:
            def do_something():
                time.sleep(0.2)
                assert client.get_analog(port_a) == value_a

            thread = client.spawn(do_something)
            assert client.get_digital(port_d) == value_d
            time.sleep(0.1)
        thread.join()


def test_daemon_context(start_dummy_sync, connect_client):
    port, value = 0, 0
    with start_dummy_sync(Commands.analog_request, port, value) as server:
        with connect_client(server) as client:
            def do_something():
                assert client.get_analog(port) == value
                time.sleep(0.2)
                with pytest.raises(errors.HedgehogCommandError):
                    client.get_analog(port)

            thread = client.spawn(do_something, daemon=True)
            time.sleep(0.1)
        thread.join()


# def test_connect(event_loop, zmq_aio_ctx: zmq.asyncio.Context, start_server_sync):
#     hardware_adapter = MockedHardwareAdapter()
#     hardware_adapter.set_digital(15, event_loop.time() - 0.1, True)
#     hardware_adapter.set_digital(15, event_loop.time() + 0.2, False)
#     with start_server_sync(hardware_adapter=hardware_adapter) as server:
#         with connect(server, emergency=15, ctx=zmq_aio_ctx) as client:
#             assert client.get_analog(0) == 0
#
#             time.sleep(0.3)
#             with pytest.raises(errors.EmergencyShutdown):
#                 assert client.get_analog(0) == 0


# def test_connect_multiple(event_loop, zmq_aio_ctx: zmq.asyncio.Context, start_server_sync):
#     hardware_adapter = MockedHardwareAdapter()
#     hardware_adapter.set_digital(15, event_loop.time() - 0.1, True)
#     hardware_adapter.set_digital(15, event_loop.time() + 0.2, False)
#     with start_server_sync(hardware_adapter=hardware_adapter) as server:
#         with connect(server, emergency=15, ctx=zmq_aio_ctx) as client1, \
#                 connect(server, emergency=15, ctx=zmq_aio_ctx) as client2:
#             assert client1.get_analog(0) == 0
#             assert client2.get_analog(0) == 0
#
#             time.sleep(0.3)
#             with pytest.raises(errors.EmergencyShutdown):
#                 assert client1.get_analog(0) == 0
#             with pytest.raises(errors.EmergencyShutdown):
#                 assert client2.get_analog(0) == 0


# tests for failures

def test_inactive_context(zmq_aio_ctx: zmq.asyncio.Context, start_dummy_sync):
    port, value = 0, 0
    with start_dummy_sync(Commands.analog_request, port, value) as server:
        client = HedgehogClient(zmq_aio_ctx, server)

        with pytest.raises(RuntimeError):
            client.get_analog(port)

        with client:
            assert client.get_analog(port) == value

        with pytest.raises(RuntimeError):
            client.get_analog(port)


def test_daemon_context_first(zmq_aio_ctx: zmq.asyncio.Context, start_dummy_sync):
    port, value = 0, 0
    with start_dummy_sync(Commands.analog_request, port, value) as server:
        client = HedgehogClient(zmq_aio_ctx, server)

        with pytest.raises(RuntimeError):
            with client.daemon:
                pass

        # confirm the client works after a failure
        with client:
            assert client.get_analog(port) == value


def test_shutdown_context(connect_dummy):
    port, value = 0, 0
    with connect_dummy(Commands.analog_request, port, value) as client:
        def do_something():
            assert client.get_analog(port) == value
            time.sleep(0.2)
            with pytest.raises(errors.EmergencyShutdown):
                client.get_analog(port)

        assert not client.is_shutdown and not client.is_closed

        thread = client.spawn(do_something, daemon=True)
        time.sleep(0.1)
        client.shutdown()

        assert client.is_shutdown and not client.is_closed

        with pytest.raises(errors.EmergencyShutdown):
            client.get_analog(port)

        thread.join()

        # this should not raise an exception out of the `with` block
        raise errors.EmergencyShutdown()

    assert client.is_shutdown and client.is_closed


def test_reuse_after_shutdown(zmq_aio_ctx: zmq.asyncio.Context, start_dummy_sync):
    port, value = 0, 0
    with start_dummy_sync(Commands.analog_request, port, value) as server:
        client = HedgehogClient(zmq_aio_ctx, server)

        with client:
            assert client.get_analog(port) == value

        with pytest.raises(RuntimeError):
            with client:
                pass

        with pytest.raises(RuntimeError):
            client.shutdown()


def test_faulty_client(zmq_aio_ctx: zmq.asyncio.Context, start_dummy_sync):
    from hedgehog.client import async_client

    port, value = 0, 0
    with start_dummy_sync(Commands.analog_request, port, value) as server:
        class MyException(Exception):
            pass

        faulty = True

        class FaultyAsyncClient(async_client.HedgehogClient):
            async def _workload(self, *, commands: PipeEnd, events: PipeEnd) -> None:
                if faulty:
                    raise MyException()
                else:
                    return await super(FaultyAsyncClient, self)._workload(commands=commands, events=events)

        class FaultyClient(HedgehogClient):
            def _create_client(self):
                return FaultyAsyncClient(self.ctx, self.endpoint)

        client = FaultyClient(zmq_aio_ctx, server)

        with pytest.raises(MyException):
            with client:
                pass

        faulty = False

        with client:
            assert client.get_analog(port) == value


def test_unsupported(connect_dummy):
    with connect_dummy(Commands.unsupported) as client:
        with pytest.raises(errors.UnsupportedCommandError):
            client.get_analog(0)


# API tests

class TestHedgehogClientAPI(object):
    def test_set_input_state(self, connect_dummy):
        port, pullup = 0, False
        with connect_dummy(Commands.io_action_input, port, pullup) as client:
            assert client.set_input_state(port, pullup) is None

    def test_get_io_config(self, connect_dummy):
        port, flags = 0, io.INPUT_FLOATING
        with connect_dummy(Commands.io_command_request, port, flags) as client:
            assert client.get_io_config(port) == flags

    def test_get_analog(self, connect_dummy):
        port, value = 0, 0
        with connect_dummy(Commands.analog_request, port, value) as client:
            assert client.get_analog(port) == value

    def test_get_digital(self, connect_dummy):
        port, value = 0, False
        with connect_dummy(Commands.digital_request, port, value) as client:
            assert client.get_digital(port) == value

    def test_set_digital_output(self, connect_dummy):
        port, level = 0, False
        with connect_dummy(Commands.io_action_output, port, level) as client:
            assert client.set_digital_output(port, level) is None

    def test_configure_motor(self, connect_dummy):
        port = 0
        with connect_dummy(Commands.motor_config_action, port, motor.DcConfig()) as client:
            assert client.configure_motor(port, motor.DcConfig()) is None

    def test_configure_motor_dc(self, connect_dummy):
        port = 0
        with connect_dummy(Commands.motor_config_action, port, motor.DcConfig()) as client:
            assert client.configure_motor_dc(port) is None

    def test_configure_motor_encoder(self, connect_dummy):
        port, encoder_a_port, encoder_b_port = 0, 0, 1
        config = motor.EncoderConfig(encoder_a_port, encoder_b_port)
        with connect_dummy(Commands.motor_config_action, port, config) as client:
            assert client.configure_motor_encoder(port, encoder_a_port, encoder_b_port) is None

    def test_configure_motor_stepper(self, connect_dummy):
        port = 0
        with connect_dummy(Commands.motor_config_action, port, motor.StepperConfig()) as client:
            assert client.configure_motor_stepper(port) is None

    def test_set_motor(self, connect_dummy):
        port, state, amount = 0, motor.POWER, 100
        with connect_dummy(Commands.motor_action, port, state, amount) as client:
            assert client.set_motor(port, state, amount) is None

        with connect_dummy(Commands.motor_action, port, state, amount) as client:
            assert client.move(port, amount) is None

    def test_get_motor_command(self, connect_dummy):
        port, state, amount = 0, motor.POWER, 0
        with connect_dummy(Commands.motor_command_request, port, state, amount) as client:
            assert client.get_motor_command(port) == (state, amount)

    def test_get_motor_state(self, connect_dummy):
        port, velocity, position = 0, 0, 0
        with connect_dummy(Commands.motor_state_request, port, velocity, position) as client:
            assert client.get_motor_state(port) == (velocity, position)

        with connect_dummy(Commands.motor_state_request, port, velocity, position) as client:
            assert client.get_motor_velocity(port) == velocity

        with connect_dummy(Commands.motor_state_request, port, velocity, position) as client:
            assert client.get_motor_position(port) == position

    def test_set_motor_position(self, connect_dummy):
        port, position = 0, 0
        with connect_dummy(Commands.motor_set_position_action, port, position) as client:
            assert client.set_motor_position(port, position) is None

    def test_set_servo(self, connect_dummy):
        port, position, raw_position = 0, None, None
        with connect_dummy(Commands.servo_action, port, raw_position) as client:
            assert client.set_servo(port, position) is None

        port, position, raw_position = 0, 0, 1000
        with connect_dummy(Commands.servo_action, port, raw_position) as client:
            assert client.set_servo(port, position) is None

        port, position, raw_position = 0, 1000, 5000
        with connect_dummy(Commands.servo_action, port, raw_position) as client:
            assert client.set_servo(port, position) is None

    def test_set_servo_raw(self, connect_dummy):
        port, raw_position = 0, None
        with connect_dummy(Commands.servo_action, port, raw_position) as client:
            assert client.set_servo_raw(port, raw_position) is None

        port, raw_position = 0, 1000
        with connect_dummy(Commands.servo_action, port, raw_position) as client:
            assert client.set_servo_raw(port, raw_position) is None

        port, raw_position = 0, 5000
        with connect_dummy(Commands.servo_action, port, raw_position) as client:
            assert client.set_servo_raw(port, raw_position) is None

    def test_get_servo_position(self, connect_dummy):
        port, position, raw_position = 0, None, None
        with connect_dummy(Commands.servo_command_request, port, raw_position) as client:
            assert client.get_servo_position(port) == position

        port, position, raw_position = 0, 0, 1000
        with connect_dummy(Commands.servo_command_request, port, raw_position) as client:
            assert client.get_servo_position(port) == position

        port, position, raw_position = 0, 1000, 5000
        with connect_dummy(Commands.servo_command_request, port, raw_position) as client:
            assert client.get_servo_position(port) == position

    def test_get_servo_position_raw(self, connect_dummy):
        port, raw_position = 0, None
        with connect_dummy(Commands.servo_command_request, port, raw_position) as client:
            assert client.get_servo_position_raw(port) == raw_position

        port, raw_position = 0, 1000
        with connect_dummy(Commands.servo_command_request, port, raw_position) as client:
            assert client.get_servo_position_raw(port) == raw_position

        port, raw_position = 0, 5000
        with connect_dummy(Commands.servo_command_request, port, raw_position) as client:
            assert client.get_servo_position_raw(port) == raw_position

    def test_get_imu_rate(self, connect_dummy):
        x, y, z = 0, 0, -100
        with connect_dummy(Commands.imu_rate_request, x, y, z) as client:
            assert client.get_imu_rate() == (x, y, z)

    def test_get_imu_acceleration(self, connect_dummy):
        x, y, z = 0, 0, -100
        with connect_dummy(Commands.imu_acceleration_request, x, y, z) as client:
            assert client.get_imu_acceleration() == (x, y, z)

    def test_get_imu_pose(self, connect_dummy):
        x, y, z = 0, 0, -100
        with connect_dummy(Commands.imu_pose_request, x, y, z) as client:
            assert client.get_imu_pose() == (x, y, z)

    def test_speaker_action(self, connect_dummy):
        frequency = 0
        with connect_dummy(Commands.speaker_action, frequency) as client:
            assert client.set_speaker(frequency) is None

        frequency = 440
        with connect_dummy(Commands.speaker_action, frequency) as client:
            assert client.set_speaker(frequency) is None


class TestHedgehogLegoClientAPI(object):
    def test_configure_lego_motor(self, connect_dummy):
        port, encoder_a_port, encoder_b_port = 1, 2, 3
        config = motor.EncoderConfig(encoder_a_port, encoder_b_port)
        with connect_dummy(Commands.motor_config_action, port, config) as client:
            assert client.configure_lego_motor(port) is None

    def test_configure_lego_sensor(self, connect_dummy):
        port, pullup = 8, True
        with connect_dummy(Commands.io_action_input, port, pullup) as client:
            assert client.configure_lego_sensor(port) is None

            with pytest.raises(ValueError):
                client.configure_lego_sensor(7)

            with pytest.raises(ValueError):
                client.configure_lego_sensor(12)


class TestHedgehogClientProcessAPI(object):
    def test_execute_process_handle_nothing(self, connect_dummy):
        pid = 2345
        with connect_dummy(Commands.execute_process_echo_asdf, pid) as client:
            assert client.execute_process('echo', 'asdf') == pid

    # def test_execute_process_handle_exit(self, connect_dummy):
    #     pid = 2346
    #     async with connect_dummy(Commands.execute_process_echo_asdf, pid) as client:
    #         exit_a, exit_b = pipe()
    #
    #         async def on_exit(_pid, exit_code):
    #             assert _pid == pid
    #             assert exit_code == 0
    #             exit_b.send(None)
    #
    #         assert client.execute_process('echo', 'asdf', on_exit=on_exit) == pid
    #
    #         await exit_a.recv()
    #
    # def test_execute_process_handle_stream(self, connect_dummy):
    #     pid = 2347
    #     with connect_dummy(Commands.execute_process_echo_asdf, pid) as client:
    #         exit_a, exit_b = pipe()
    #
    #         counter = 0
    #
    #         async def on_stdout(_pid, fileno, chunk):
    #             nonlocal counter
    #
    #             expect = [
    #                 (pid, process.STDOUT, b'asdf\n'),
    #                 (pid, process.STDOUT, b''),
    #             ]
    #
    #             assert (_pid, fileno, chunk) == expect[counter]
    #             counter += 1
    #
    #             if counter == len(expect):
    #                 await exit_b.send(None)
    #
    #         assert client.execute_process('echo', 'asdf', on_stdout=on_stdout) == pid
    #
    #         await exit_a.recv()
    #
    # def test_execute_process_handle_input(self, connect_dummy):
    #     pid = 2348
    #     with connect_dummy(Commands.execute_process_cat, pid) as client:
    #         assert client.execute_process('cat') == pid
    #         assert client.send_process_data(pid, b'asdf\n') is None
    #         assert client.send_process_data(pid) is None
