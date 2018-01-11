from typing import Awaitable, Callable

import pytest
from hedgehog.utils.test_utils import event_loop, zmq_aio_ctx
from hedgehog.client.test_utils import handler, hardware_adapter, start_dummy, Commands

import asyncio
import zmq.asyncio
from aiostream.context_utils import async_context_manager

from hedgehog.client.async_client import HedgehogClient
from hedgehog.protocol import errors
from hedgehog.protocol.messages import Message, ack, analog, digital, io, motor, servo, process
from hedgehog.protocol.async_sockets import DealerRouterSocket
from hedgehog.server import HedgehogServer
from hedgehog.server.hardware import HardwareAdapter
from hedgehog.server.hardware.mocked import MockedHardwareAdapter
from hedgehog.utils.asyncio import pipe

# Pytest fixtures
event_loop, zmq_aio_ctx, hardware_adapter


@async_context_manager
async def connect_dummy(ctx: zmq.asyncio.Context, dummy: Callable[[DealerRouterSocket], Awaitable[None]], *args,
                        endpoint: str='inproc://controller', client_class=HedgehogClient, **kwargs):
    async with start_dummy(ctx, dummy, *args, endpoint=endpoint, **kwargs):
        async with client_class(ctx, endpoint) as client:
            yield client


@pytest.fixture
async def server(zmq_aio_ctx: zmq.asyncio.Context, hardware_adapter: HardwareAdapter):
    async with HedgehogServer(zmq_aio_ctx, 'inproc://controller', handler(hardware_adapter)) as server:
        yield 'inproc://controller'


@pytest.fixture
async def client(zmq_aio_ctx: zmq.asyncio.Context, server: str):
    async with HedgehogClient(zmq_aio_ctx, server) as client:
        yield client


@pytest.mark.asyncio
async def test_concurrent_commands(client: HedgehogClient, hardware_adapter: MockedHardwareAdapter):
    hardware_adapter.set_digital(8, 0, True)

    task_a = asyncio.ensure_future(client.get_analog(0))
    task_b = asyncio.ensure_future(client.get_digital(8))
    assert await task_a == 0
    assert await task_b is True


@pytest.mark.asyncio
async def test_overlapping_contexts(zmq_aio_ctx: zmq.asyncio.Context, server: str):
    async with HedgehogClient(zmq_aio_ctx, server) as client:
        async def do_something():
            assert client._open_count == 2
            await asyncio.sleep(2)
            assert client._open_count == 1
            assert await client.get_analog(0) == 0

        assert client._open_count == 1
        task = await client.spawn(do_something())
        assert client._open_count == 2
        assert await client.get_analog(0) == 0
        await asyncio.sleep(1)
    await task


@pytest.mark.asyncio
async def test_inactive_context(zmq_aio_ctx: zmq.asyncio.Context, server: str):
    client = HedgehogClient(zmq_aio_ctx, server)

    with pytest.raises(RuntimeError):
        await client.get_analog(0)

    async with client:
        assert await client.get_analog(0) == 0

    with pytest.raises(RuntimeError):
        await client.get_analog(0)


@pytest.mark.asyncio
async def test_shutdown_context(client: HedgehogClient):
    async def do_something():
        assert await client.get_analog(0) == 0
        await asyncio.sleep(2)
        with pytest.raises(errors.HedgehogCommandError):
            await client.get_analog(0)

    task = await client.spawn(do_something())

    assert await client.get_analog(0) == 0
    await asyncio.sleep(1)
    await client.shutdown()

    with pytest.raises(errors.HedgehogCommandError):
        await client.get_analog(0)

    await task


@pytest.mark.asyncio
async def test_daemon_context(zmq_aio_ctx: zmq.asyncio.Context, server: str):
    async with HedgehogClient(zmq_aio_ctx, server) as client:
        async def do_something():
            assert await client.get_analog(0) == 0
            await asyncio.sleep(2)
            with pytest.raises(errors.HedgehogCommandError):
                await client.get_analog(0)

        task = await client.spawn(do_something(), daemon=True)
        await asyncio.sleep(1)
    await task


@pytest.mark.asyncio
@pytest.mark.parametrize('hardware_adapter', [HardwareAdapter()])
async def test_unsupported(client: HedgehogClient):
    with pytest.raises(errors.UnsupportedCommandError):
        await client.get_analog(0)


class HedgehogAPITestCase(object):
    @pytest.fixture
    def connect(self, zmq_aio_ctx, command):
        @async_context_manager
        async def do_connect(*args, **kwargs):
            async with connect_dummy(zmq_aio_ctx, command, *args,
                                     **kwargs) as client:
                yield client

        return do_connect


class TestHedgehogClientAPI(HedgehogAPITestCase):
    @pytest.mark.asyncio
    @pytest.mark.parametrize('command', [Commands.io_action_input])
    async def test_set_input_state(self, connect):
        port, pullup = 0, False
        async with connect(port, pullup) as client:
            assert await client.set_input_state(port, pullup) is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize('command', [Commands.io_command_request])
    async def test_get_io_config(self, connect):
        port, flags = 0, io.INPUT_FLOATING
        async with connect(port, flags) as client:
            assert await client.get_io_config(port) == flags

    @pytest.mark.asyncio
    @pytest.mark.parametrize('command', [Commands.analog_request])
    async def test_get_analog(self, connect):
        port, value = 0, 0
        async with connect(port, value) as client:
            assert await client.get_analog(port) == value

    @pytest.mark.asyncio
    @pytest.mark.parametrize('command', [Commands.digital_request])
    async def test_get_digital(self, connect):
        port, value = 0, False
        async with connect(port, value) as client:
            assert await client.get_digital(port) == value

    @pytest.mark.asyncio
    @pytest.mark.parametrize('command', [Commands.io_action_output])
    async def test_set_digital_output(self, connect):
        port, level = 0, False
        async with connect(port, level) as client:
            assert await client.set_digital_output(port, level) is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize('command', [Commands.motor_action])
    async def test_set_motor(self, connect):
        port, state, amount = 0, motor.POWER, 100
        async with connect(port, state, amount) as client:
            assert await client.set_motor(port, state, amount) is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize('command', [Commands.motor_command_request])
    async def test_get_motor_command(self, connect):
        port, state, amount = 0, motor.POWER, 0
        async with connect(port, state, amount) as client:
            assert await client.get_motor_command(port) == (state, amount)

    @pytest.mark.asyncio
    @pytest.mark.parametrize('command', [Commands.motor_state_request])
    async def test_get_motor_state(self, connect):
        port, velocity, position = 0, 0, 0
        async with connect(port, velocity, position) as client:
            assert await client.get_motor_state(port) == (velocity, position)

    @pytest.mark.asyncio
    @pytest.mark.parametrize('command', [Commands.motor_set_position_action])
    async def test_set_motor_position(self, connect):
        port, position = 0, 0
        async with connect(port, position) as client:
            assert await client.set_motor_position(port, position) is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize('command', [Commands.servo_action])
    async def test_set_servo(self, connect):
        port, active, position = 0, False, 0
        async with connect(port, active, position) as client:
            assert await client.set_servo(port, active, position) is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize('command', [Commands.servo_command_request])
    async def test_get_servo_command(self, connect):
        port, active, position = 0, False, None
        async with connect(port, active, position) as client:
            assert await client.get_servo_command(port) == (active, position)

        port, active, position = 0, True, 0
        async with connect(port, active, position) as client:
            assert await client.get_servo_command(port) == (active, position)


class TestHedgehogClientProcessAPI(HedgehogAPITestCase):
    @pytest.mark.asyncio
    @pytest.mark.parametrize('command', [Commands.execute_process_echo_asdf])
    async def test_execute_process_handle_nothing(self, connect):
        pid = 2345
        async with connect(pid) as client:
            assert await client.execute_process('echo', 'asdf') == pid

    @pytest.mark.asyncio
    @pytest.mark.parametrize('command', [Commands.execute_process_echo_asdf])
    async def test_execute_process_handle_exit(self, connect):
        pid = 2346
        async with connect(pid) as client:
            exit_a, exit_b = pipe()

            async def on_exit(_pid, exit_code):
                assert _pid == pid
                assert exit_code == 0
                await exit_b.send(None)

            assert await client.execute_process('echo', 'asdf', on_exit=on_exit) == pid

            await exit_a.recv()

    @pytest.mark.asyncio
    @pytest.mark.parametrize('command', [Commands.execute_process_echo_asdf])
    async def test_execute_process_handle_stream(self, connect):
        pid = 2347
        async with connect(pid) as client:
            exit_a, exit_b = pipe()

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
                    await exit_b.send(None)

            assert await client.execute_process('echo', 'asdf', on_stdout=on_stdout) == pid

            await exit_a.recv()

    @pytest.mark.asyncio
    @pytest.mark.parametrize('command', [Commands.execute_process_cat])
    async def test_execute_process_handle_input(self, connect):
        pid = 2348
        async with connect(pid) as client:
            assert await client.execute_process('cat') == pid
            assert await client.send_process_data(pid, b'asdf\n') is None
            assert await client.send_process_data(pid) is None
