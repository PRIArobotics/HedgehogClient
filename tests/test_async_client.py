from typing import Awaitable, Callable

import pytest
from hedgehog.utils.test_utils import event_loop, zmq_aio_ctx
from hedgehog.client.test_utils import start_dummy, Commands

import asyncio
from contextlib import asynccontextmanager
import zmq.asyncio

from concurrent_utils.pipe import PipeEnd
from hedgehog.client.async_client import HedgehogClient, connect
from hedgehog.protocol import errors
from hedgehog.protocol.messages import io, motor, process, vision
from hedgehog.protocol.zmq.asyncio import DealerRouterSocket

# Pytest fixtures
event_loop, zmq_aio_ctx, start_dummy


# additional fixtures

@pytest.fixture
def connect_client(zmq_aio_ctx: zmq.asyncio.Context):
    @asynccontextmanager
    async def do_connect(endpoint, client_class=HedgehogClient):
        async with client_class(zmq_aio_ctx, endpoint) as client:
            yield client

    return do_connect


@pytest.fixture
def connect_dummy(start_dummy, connect_client):
    @asynccontextmanager
    async def do_connect(server_coro: Callable[[DealerRouterSocket], Awaitable[None]], *args,
                         endpoint: str='inproc://controller', client_class=HedgehogClient, **kwargs):
        async with start_dummy(server_coro, *args, endpoint=endpoint, **kwargs) as dummy, \
                connect_client(dummy, client_class=client_class) as client:
            yield client

    return do_connect


# tests

@pytest.mark.asyncio
async def test_multipart_commands(connect_dummy):
    port_a, state_a, amount_a, port_b, state_b, amount_b = 0, motor.POWER, 100, 3, motor.POWER, 100
    async with connect_dummy(Commands.multipart_motor_requests, port_a, state_a, amount_a, port_b, state_b, amount_b) as client:
        await client.commands(
            client.move_motor_cmd(port_a, amount_a, state_a),
            client.move_motor_cmd(port_b, amount_b, state_b),
        )

    port_a, state_a, amount_a, port_b, state_b, amount_b = 0, motor.POWER, 100, 4, motor.POWER, 100
    async with connect_dummy(Commands.multipart_motor_requests, port_a, state_a, amount_a, port_b, state_b, amount_b) as client:
        with pytest.raises(errors.FailedCommandError):
            await client.commands(
                client.move_motor_cmd(port_a, amount_a, state_a),
                client.move_motor_cmd(port_b, amount_b, state_b),
            )


@pytest.mark.asyncio
async def test_concurrent_commands(connect_dummy):
    port_a, value_a, port_d, value_d = 0, 0, 0, True
    async with connect_dummy(Commands.concurrent_analog_digital_requests, port_a, value_a, port_d, value_d) as client:
        task_a = asyncio.ensure_future(client.get_analog(port_a))
        task_d = asyncio.ensure_future(client.get_digital(port_d))
        assert await task_a == value_a
        assert await task_d is value_d


@pytest.mark.asyncio
async def test_overlapping_contexts(start_dummy, connect_client):
    port_a, value_a, port_d, value_d = 0, 0, 0, True
    async with start_dummy(Commands.concurrent_analog_digital_requests, port_a, value_a, port_d, value_d) as server:
        async with connect_client(server) as client:
            async def do_something():
                assert client._open_count == 2
                await asyncio.sleep(2)
                # TODO this assertion should hold, but doesn't
                # it's probably to do with the simulated loop's time
                #assert client._open_count == 1
                assert await client.get_analog(port_a) == value_a

            assert client._open_count == 1
            task = await client.spawn(do_something())
            assert client._open_count == 2
            assert await client.get_digital(port_d) == value_d
            await asyncio.sleep(1)
        await task


@pytest.mark.asyncio
async def test_daemon_context(start_dummy, connect_client):
    port, value = 0, 0
    async with start_dummy(Commands.analog_request, port, value) as server:
        async with connect_client(server) as client:
            async def do_something():
                assert await client.get_analog(port) == value
                await asyncio.sleep(2)
                with pytest.raises(errors.HedgehogCommandError):
                    await client.get_analog(port)

            task = await client.spawn(do_something(), daemon=True)
            await asyncio.sleep(1)
        await task


# @pytest.mark.asyncio
# async def test_connect(zmq_aio_ctx: zmq.asyncio.Context, start_server):
#     hardware_adapter = MockedHardwareAdapter()
#     hardware_adapter.set_digital(15, 0, True)
#     hardware_adapter.set_digital(15, 1, False)
#     async with start_server(hardware_adapter=hardware_adapter) as server:
#         async with connect(server, emergency=15, ctx=zmq_aio_ctx) as client:
#             assert await client.get_analog(0) == 0
#
#             await asyncio.sleep(2)
#             # signals don't play nicely with the simulated time of the loop.
#             # sleep again after the signal interrupted the original sleep.
#             await asyncio.sleep(1)
#             with pytest.raises(errors.EmergencyShutdown):
#                 assert await client.get_analog(0) == 0


# @pytest.mark.asyncio
# async def test_connect_multiple(zmq_aio_ctx: zmq.asyncio.Context, start_server):
#     hardware_adapter = MockedHardwareAdapter()
#     hardware_adapter.set_digital(15, 0, True)
#     hardware_adapter.set_digital(15, 1, False)
#     async with start_server(hardware_adapter=hardware_adapter) as server:
#         async with connect(server, emergency=15, ctx=zmq_aio_ctx) as client1, \
#                 connect(server, emergency=15, ctx=zmq_aio_ctx) as client2:
#             assert await client1.get_analog(0) == 0
#             assert await client2.get_analog(0) == 0
#
#             await asyncio.sleep(2)
#             # signals don't play nicely with the simulated time of the loop.
#             # sleep again after the signal interrupted the original sleep.
#             await asyncio.sleep(1)
#             with pytest.raises(errors.EmergencyShutdown):
#                 assert await client1.get_analog(0) == 0
#             with pytest.raises(errors.EmergencyShutdown):
#                 assert await client2.get_analog(0) == 0


# tests for failures

@pytest.mark.asyncio
async def test_inactive_context(zmq_aio_ctx: zmq.asyncio.Context, start_dummy):
    port, value = 0, 0
    async with start_dummy(Commands.analog_request, port, value) as server:
        client = HedgehogClient(zmq_aio_ctx, server)

        with pytest.raises(RuntimeError):
            await client.get_analog(port)

        async with client:
            assert await client.get_analog(port) == value

        with pytest.raises(RuntimeError):
            await client.get_analog(port)


@pytest.mark.asyncio
async def test_daemon_context_first(zmq_aio_ctx: zmq.asyncio.Context, start_dummy):
    port, value = 0, 0
    async with start_dummy(Commands.analog_request, port, value) as server:
        client = HedgehogClient(zmq_aio_ctx, server)

        with pytest.raises(RuntimeError):
            async with client.daemon:
                pass

        # confirm the client works after a failure
        async with client:
            assert await client.get_analog(port) == value


@pytest.mark.asyncio
async def test_shutdown_context(connect_dummy):
    port, value = 0, 0
    async with connect_dummy(Commands.analog_request, port, value) as client:
        async def do_something():
            assert await client.get_analog(port) == value
            await asyncio.sleep(2)
            with pytest.raises(errors.EmergencyShutdown):
                await client.get_analog(port)

            # this should not raise an exception into `await task`
            raise errors.EmergencyShutdown()

        assert not client.is_shutdown and not client.is_closed

        task = await client.spawn(do_something())
        await asyncio.sleep(1)
        await client.shutdown()

        assert client.is_shutdown and not client.is_closed

        with pytest.raises(errors.EmergencyShutdown):
            await client.get_analog(port)

        # this should not raise an exception from `do_something`
        await task

        # this should not raise an exception out of the `async with` block
        raise errors.EmergencyShutdown()

    assert client.is_shutdown and client.is_closed


@pytest.mark.asyncio
async def test_reuse_after_shutdown(zmq_aio_ctx: zmq.asyncio.Context, start_dummy):
    port, value = 0, 0
    async with start_dummy(Commands.analog_request, port, value) as server:
        client = HedgehogClient(zmq_aio_ctx, server)

        async with client:
            assert await client.get_analog(port) == value

        with pytest.raises(RuntimeError):
            async with client:
                pass

        with pytest.raises(RuntimeError):
            await client.shutdown()


@pytest.mark.asyncio
async def test_faulty_client(zmq_aio_ctx: zmq.asyncio.Context, start_dummy):
    port, value = 0, 0
    async with start_dummy(Commands.analog_request, port, value) as server:
        class MyException(Exception):
            pass

        faulty = True

        class FaultyClient(HedgehogClient):
            async def _workload(self, *, commands: PipeEnd, events: PipeEnd) -> None:
                if faulty:
                    raise MyException()
                else:
                    return await super(FaultyClient, self)._workload(commands=commands, events=events)

        client = FaultyClient(zmq_aio_ctx, server)

        with pytest.raises(MyException):
            async with client:
                pass

        assert client._open_count == 0
        faulty = False

        async with client:
            assert await client.get_analog(port) == value


@pytest.mark.asyncio
async def test_unsupported(connect_dummy):
    async with connect_dummy(Commands.unsupported) as client:
        with pytest.raises(errors.UnsupportedCommandError):
            await client.get_analog(0)


# API tests

class TestHedgehogClientAPI(object):
    @pytest.mark.asyncio
    async def test_set_input_state(self, connect_dummy):
        port, pullup = 0, False
        async with connect_dummy(Commands.io_action_input, port, pullup) as client:
            assert await client.set_input_state(port, pullup) is None

    @pytest.mark.asyncio
    async def test_get_io_config(self, connect_dummy):
        port, flags = 0, io.INPUT_FLOATING
        async with connect_dummy(Commands.io_command_request, port, flags) as client:
            assert await client.get_io_config(port) == flags

    @pytest.mark.asyncio
    async def test_get_analog(self, connect_dummy):
        port, value = 0, 0
        async with connect_dummy(Commands.analog_request, port, value) as client:
            assert await client.get_analog(port) == value

    @pytest.mark.asyncio
    async def test_get_digital(self, connect_dummy):
        port, value = 0, False
        async with connect_dummy(Commands.digital_request, port, value) as client:
            assert await client.get_digital(port) == value

    @pytest.mark.asyncio
    async def test_set_digital_output(self, connect_dummy):
        port, level = 0, False
        async with connect_dummy(Commands.io_action_output, port, level) as client:
            assert await client.set_digital_output(port, level) is None

    @pytest.mark.asyncio
    async def test_configure_motor(self, connect_dummy):
        port = 0
        async with connect_dummy(Commands.motor_config_action, port, motor.DcConfig()) as client:
            assert await client.configure_motor(port, motor.DcConfig()) is None

    @pytest.mark.asyncio
    async def test_configure_motor_dc(self, connect_dummy):
        port = 0
        async with connect_dummy(Commands.motor_config_action, port, motor.DcConfig()) as client:
            assert await client.configure_motor_dc(port) is None

    @pytest.mark.asyncio
    async def test_configure_motor_encoder(self, connect_dummy):
        port, encoder_a_port, encoder_b_port = 0, 0, 1
        config = motor.EncoderConfig(encoder_a_port, encoder_b_port)
        async with connect_dummy(Commands.motor_config_action, port, config) as client:
            assert await client.configure_motor_encoder(port, encoder_a_port, encoder_b_port) is None

    @pytest.mark.asyncio
    async def test_configure_motor_stepper(self, connect_dummy):
        port = 0
        async with connect_dummy(Commands.motor_config_action, port, motor.StepperConfig()) as client:
            assert await client.configure_motor_stepper(port) is None

    @pytest.mark.asyncio
    async def test_move_motor(self, connect_dummy):
        port, state, amount = 0, motor.POWER, 100
        async with connect_dummy(Commands.motor_action, port, state, amount) as client:
            assert await client.move_motor(port, amount) is None

    @pytest.mark.asyncio
    async def test_motor_off(self, connect_dummy):
        port, state, amount = 0, motor.POWER, 0
        async with connect_dummy(Commands.motor_action, port, state, amount) as client:
            assert await client.motor_off(port) is None

    @pytest.mark.asyncio
    async def test_brake(self, connect_dummy):
        port, state, amount = 0, motor.BRAKE, 1000
        async with connect_dummy(Commands.motor_action, port, state, amount) as client:
            assert await client.brake(port) is None

    @pytest.mark.asyncio
    async def test_get_motor_command(self, connect_dummy):
        port, state, amount = 0, motor.POWER, 0
        async with connect_dummy(Commands.motor_command_request, port, state, amount) as client:
            assert await client.get_motor_command(port) == (state, amount)

    @pytest.mark.asyncio
    async def test_get_motor_state(self, connect_dummy):
        port, velocity, position = 0, 0, 0
        async with connect_dummy(Commands.motor_state_request, port, velocity, position) as client:
            assert await client.get_motor_state(port) == (velocity, position)

        async with connect_dummy(Commands.motor_state_request, port, velocity, position) as client:
            assert await client.get_motor_velocity(port) == velocity

        async with connect_dummy(Commands.motor_state_request, port, velocity, position) as client:
            assert await client.get_motor_position(port) == position

    @pytest.mark.asyncio
    async def test_set_motor_position(self, connect_dummy):
        port, position = 0, 0
        async with connect_dummy(Commands.motor_set_position_action, port, position) as client:
            assert await client.set_motor_position(port, position) is None

    @pytest.mark.asyncio
    async def test_set_servo(self, connect_dummy):
        port, position, raw_position = 0, None, None
        async with connect_dummy(Commands.servo_action, port, raw_position) as client:
            assert await client.set_servo(port, position) is None

        port, position, raw_position = 0, 0, 1000
        async with connect_dummy(Commands.servo_action, port, raw_position) as client:
            assert await client.set_servo(port, position) is None

        port, position, raw_position = 0, 1000, 5000
        async with connect_dummy(Commands.servo_action, port, raw_position) as client:
            assert await client.set_servo(port, position) is None

    @pytest.mark.asyncio
    async def test_set_servo_raw(self, connect_dummy):
        port, raw_position = 0, None
        async with connect_dummy(Commands.servo_action, port, raw_position) as client:
            assert await client.set_servo_raw(port, raw_position) is None

        port, raw_position = 0, 1000
        async with connect_dummy(Commands.servo_action, port, raw_position) as client:
            assert await client.set_servo_raw(port, raw_position) is None

        port, raw_position = 0, 5000
        async with connect_dummy(Commands.servo_action, port, raw_position) as client:
            assert await client.set_servo_raw(port, raw_position) is None

    @pytest.mark.asyncio
    async def test_get_servo_position(self, connect_dummy):
        port, position, raw_position = 0, None, None
        async with connect_dummy(Commands.servo_command_request, port, raw_position) as client:
            assert await client.get_servo_position(port) == position

        port, position, raw_position = 0, 0, 1002
        async with connect_dummy(Commands.servo_command_request, port, raw_position) as client:
            assert await client.get_servo_position(port) == position

        port, position, raw_position = 0, 1000, 5000
        async with connect_dummy(Commands.servo_command_request, port, raw_position) as client:
            assert await client.get_servo_position(port) == position

    @pytest.mark.asyncio
    async def test_get_servo_position_raw(self, connect_dummy):
        port, raw_position = 0, None
        async with connect_dummy(Commands.servo_command_request, port, raw_position) as client:
            assert await client.get_servo_position_raw(port) == raw_position

        port, raw_position = 0, 1000
        async with connect_dummy(Commands.servo_command_request, port, raw_position) as client:
            assert await client.get_servo_position_raw(port) == raw_position

        port, raw_position = 0, 5000
        async with connect_dummy(Commands.servo_command_request, port, raw_position) as client:
            assert await client.get_servo_position_raw(port) == raw_position

    @pytest.mark.asyncio
    async def test_get_imu_rate(self, connect_dummy):
        x, y, z = 0, 0, -100
        async with connect_dummy(Commands.imu_rate_request, x, y, z) as client:
            assert await client.get_imu_rate() == (x, y, z)

    @pytest.mark.asyncio
    async def test_get_imu_acceleration(self, connect_dummy):
        x, y, z = 0, 0, -100
        async with connect_dummy(Commands.imu_acceleration_request, x, y, z) as client:
            assert await client.get_imu_acceleration() == (x, y, z)

    @pytest.mark.asyncio
    async def test_get_imu_pose(self, connect_dummy):
        x, y, z = 0, 0, -100
        async with connect_dummy(Commands.imu_pose_request, x, y, z) as client:
            assert await client.get_imu_pose() == (x, y, z)

    @pytest.mark.asyncio
    async def test_speaker_action(self, connect_dummy):
        frequency = None
        async with connect_dummy(Commands.speaker_action, frequency) as client:
            assert await client.set_speaker(frequency) is None

        frequency = 440
        async with connect_dummy(Commands.speaker_action, frequency) as client:
            assert await client.set_speaker(frequency) is None

    @pytest.mark.asyncio
    async def test_open_camera(self, connect_dummy):
        async with connect_dummy(Commands.open_camera_action) as client:
            assert await client.open_camera() is None

    @pytest.mark.asyncio
    async def test_close_camera(self, connect_dummy):
        async with connect_dummy(Commands.close_camera_action) as client:
            assert await client.close_camera() is None

    @pytest.mark.asyncio
    async def test_camera(self, connect_dummy):
        async with connect_dummy(Commands.camera_context) as client:
            async with client.camera():
                pass

    @pytest.mark.asyncio
    async def test_create_channel(self, connect_dummy):
        key, channel = 'foo', vision.FacesChannel()
        async with connect_dummy(Commands.create_channel_action, key, channel) as client:
            assert await client.create_channel(key, channel) is None

    @pytest.mark.asyncio
    async def test_update_channel(self, connect_dummy):
        key, channel = 'foo', vision.FacesChannel()
        async with connect_dummy(Commands.update_channel_action, key, channel) as client:
            assert await client.update_channel(key, channel) is None

    @pytest.mark.asyncio
    async def test_delete_channel(self, connect_dummy):
        key = 'foo'
        async with connect_dummy(Commands.delete_channel_action, key) as client:
            assert await client.delete_channel(key) is None

    @pytest.mark.asyncio
    async def test_get_channel(self, connect_dummy):
        key, channel = 'foo', vision.FacesChannel()
        async with connect_dummy(Commands.channel_request, key, channel) as client:
            assert await client.get_channel(key) == channel

    @pytest.mark.asyncio
    async def test_get_channels(self, connect_dummy):
        key, channel = 'foo', vision.FacesChannel()
        async with connect_dummy(Commands.channel_request_list, key, channel) as client:
            assert await client.get_channels() == {key: channel}

    @pytest.mark.asyncio
    async def test_capture_frame(self, connect_dummy):
        async with connect_dummy(Commands.capture_frame_action) as client:
            assert await client.capture_frame() is None

    @pytest.mark.asyncio
    async def test_get_frame(self, connect_dummy):
        highlight, frame = 'foo', b'asdf'
        async with connect_dummy(Commands.frame_request, highlight, frame) as client:
            assert await client.get_frame(highlight) == frame

    @pytest.mark.asyncio
    async def test_get_feature(self, connect_dummy):
        channel, frame = 'foo', vision.FacesFeature([])
        async with connect_dummy(Commands.feature_request, channel, frame) as client:
            assert await client.get_feature(channel) == frame


class TestHedgehogLegoClientAPI(object):
    @pytest.mark.asyncio
    async def test_configure_lego_motor(self, connect_dummy):
        port, encoder_a_port, encoder_b_port = 1, 2, 3
        config = motor.EncoderConfig(encoder_a_port, encoder_b_port)
        async with connect_dummy(Commands.motor_config_action, port, config) as client:
            assert await client.configure_lego_motor(port) is None

    @pytest.mark.asyncio
    async def test_configure_lego_sensor(self, connect_dummy):
        port, pullup = 8, True
        async with connect_dummy(Commands.io_action_input, port, pullup) as client:
            assert await client.configure_lego_sensor(port) is None

            with pytest.raises(ValueError):
                await client.configure_lego_sensor(7)

            with pytest.raises(ValueError):
                await client.configure_lego_sensor(12)


class TestHedgehogClientProcessAPI(object):
    @pytest.mark.asyncio
    async def test_execute_process_handle_nothing(self, connect_dummy):
        pid = 2345
        async with connect_dummy(Commands.execute_process_echo_asdf, pid) as client:
            assert await client.execute_process('echo', 'asdf') == pid

    @pytest.mark.asyncio
    async def test_execute_process_handle_exit(self, connect_dummy):
        pid = 2346
        async with connect_dummy(Commands.execute_process_echo_asdf, pid) as client:
            event = asyncio.Event()

            async def on_exit(_pid, exit_code):
                assert _pid == pid
                assert exit_code == 0
                event.set()

            assert await client.execute_process('echo', 'asdf', on_exit=on_exit) == pid

            await event.wait()

    @pytest.mark.asyncio
    async def test_execute_process_handle_stream(self, connect_dummy):
        pid = 2347
        async with connect_dummy(Commands.execute_process_echo_asdf, pid) as client:
            event = asyncio.Event()

            counter = 0

            async def on_stdout(_pid, fileno, chunk):
                nonlocal counter

                expect = [
                    (pid, process.STDOUT, b'asdf\n'),
                    (pid, process.STDOUT, b''),
                ]

                assert (_pid, fileno, chunk) == expect[counter]
                counter += 1

                if counter == len(expect):
                    event.set()

            assert await client.execute_process('echo', 'asdf', on_stdout=on_stdout) == pid

            await event.wait()

    @pytest.mark.asyncio
    async def test_execute_process_handle_input(self, connect_dummy):
        pid = 2348
        async with connect_dummy(Commands.execute_process_cat, pid) as client:
            assert await client.execute_process('cat') == pid
            assert await client.send_process_data(pid, b'asdf\n') is None
            assert await client.send_process_data(pid) is None
