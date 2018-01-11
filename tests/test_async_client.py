from typing import Awaitable, Callable

import pytest
from hedgehog.utils.test_utils import event_loop, zmq_aio_ctx
from hedgehog.client.test_utils import start_dummy, start_server, Commands

import asyncio
import zmq.asyncio
from aiostream.context_utils import async_context_manager

from hedgehog.client.async_client import HedgehogClient
from hedgehog.protocol import errors
from hedgehog.protocol.messages import io, motor, process
from hedgehog.protocol.async_sockets import DealerRouterSocket
from hedgehog.server.hardware import HardwareAdapter
from hedgehog.server.hardware.mocked import MockedHardwareAdapter
from hedgehog.utils.asyncio import pipe

# Pytest fixtures
event_loop, zmq_aio_ctx, start_dummy, start_server


@pytest.fixture
def connect_client(zmq_aio_ctx: zmq.asyncio.Context):
    @async_context_manager
    async def do_connect(endpoint, client_class=HedgehogClient):
        async with client_class(zmq_aio_ctx, endpoint) as client:
            yield client

    return do_connect


@pytest.fixture
def connect_dummy(start_dummy, connect_client):
    @async_context_manager
    async def do_connect(server_coro: Callable[[DealerRouterSocket], Awaitable[None]], *args,
                         endpoint: str='inproc://controller', client_class=HedgehogClient, **kwargs):
        async with start_dummy(server_coro, *args, endpoint=endpoint, **kwargs) as dummy, \
                connect_client(dummy, client_class=client_class) as client:
            yield client

    return do_connect


@pytest.fixture
def connect_server(start_server, connect_client):
    @async_context_manager
    async def do_connect(hardware_adapter: HardwareAdapter=None, endpoint: str='inproc://controller',
                         client_class=HedgehogClient):
        async with start_server(hardware_adapter=hardware_adapter, endpoint=endpoint) as server, \
                connect_client(server, client_class=client_class) as client:
            yield client

    return do_connect


@pytest.mark.asyncio
async def test_concurrent_commands(connect_server):
    hardware_adapter = MockedHardwareAdapter()
    hardware_adapter.set_digital(8, 0, True)
    async with connect_server(hardware_adapter=hardware_adapter) as client:
        task_a = asyncio.ensure_future(client.get_analog(0))
        task_b = asyncio.ensure_future(client.get_digital(8))
        assert await task_a == 0
        assert await task_b is True


@pytest.mark.asyncio
async def test_overlapping_contexts(start_server, connect_client):
    async with start_server() as server:
        async with connect_client(server) as client:
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
async def test_inactive_context(zmq_aio_ctx: zmq.asyncio.Context, start_server):
    async with start_server() as server:
        client = HedgehogClient(zmq_aio_ctx, server)

        with pytest.raises(RuntimeError):
            await client.get_analog(0)

        async with client:
            assert await client.get_analog(0) == 0

        with pytest.raises(RuntimeError):
            await client.get_analog(0)


@pytest.mark.asyncio
async def test_shutdown_context(connect_server):
    async with connect_server() as client:
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
async def test_daemon_context(start_server, connect_client):
    async with start_server() as server:
        async with connect_client(server) as client:
            async def do_something():
                assert await client.get_analog(0) == 0
                await asyncio.sleep(2)
                with pytest.raises(errors.HedgehogCommandError):
                    await client.get_analog(0)

            task = await client.spawn(do_something(), daemon=True)
            await asyncio.sleep(1)
        await task


@pytest.mark.asyncio
async def test_unsupported(connect_server):
    async with connect_server(hardware_adapter=HardwareAdapter()) as client:
        with pytest.raises(errors.UnsupportedCommandError):
            await client.get_analog(0)


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
    async def test_set_motor(self, connect_dummy):
        port, state, amount = 0, motor.POWER, 100
        async with connect_dummy(Commands.motor_action, port, state, amount) as client:
            assert await client.set_motor(port, state, amount) is None

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

    @pytest.mark.asyncio
    async def test_set_motor_position(self, connect_dummy):
        port, position = 0, 0
        async with connect_dummy(Commands.motor_set_position_action, port, position) as client:
            assert await client.set_motor_position(port, position) is None

    @pytest.mark.asyncio
    async def test_set_servo(self, connect_dummy):
        port, active, position = 0, False, 0
        async with connect_dummy(Commands.servo_action, port, active, position) as client:
            assert await client.set_servo(port, active, position) is None

    @pytest.mark.asyncio
    async def test_get_servo_command(self, connect_dummy):
        port, active, position = 0, False, None
        async with connect_dummy(Commands.servo_command_request, port, active, position) as client:
            assert await client.get_servo_command(port) == (active, position)

        port, active, position = 0, True, 0
        async with connect_dummy(Commands.servo_command_request, port, active, position) as client:
            assert await client.get_servo_command(port) == (active, position)


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
            exit_a, exit_b = pipe()

            async def on_exit(_pid, exit_code):
                assert _pid == pid
                assert exit_code == 0
                await exit_b.send(None)

            assert await client.execute_process('echo', 'asdf', on_exit=on_exit) == pid

            await exit_a.recv()

    @pytest.mark.asyncio
    async def test_execute_process_handle_stream(self, connect_dummy):
        pid = 2347
        async with connect_dummy(Commands.execute_process_echo_asdf, pid) as client:
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
    async def test_execute_process_handle_input(self, connect_dummy):
        pid = 2348
        async with connect_dummy(Commands.execute_process_cat, pid) as client:
            assert await client.execute_process('cat') == pid
            assert await client.send_process_data(pid, b'asdf\n') is None
            assert await client.send_process_data(pid) is None
