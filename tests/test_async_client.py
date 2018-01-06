from typing import Awaitable, Callable, List

import pytest
from hedgehog.utils.test_utils import event_loop, zmq_ctx, zmq_aio_ctx

import asyncio
import threading
import traceback
import zmq.asyncio
from contextlib import contextmanager
from aiostream.context_utils import async_context_manager

from hedgehog.client.async_client import AsyncClient
from hedgehog.protocol import errors, ServerSide
from hedgehog.protocol.messages import Message, ack, analog, digital, io, motor, servo, process
from hedgehog.protocol.async_sockets import DealerRouterSocket
from hedgehog.server import handlers, HedgehogServer
from hedgehog.server.handlers.hardware import HardwareHandler
from hedgehog.server.handlers.process import ProcessHandler
from hedgehog.server.hardware import HardwareAdapter
from hedgehog.server.hardware.mocked import MockedHardwareAdapter
from hedgehog.utils.asyncio import pipe

# Pytest fixtures
event_loop, zmq_ctx, zmq_aio_ctx


def handler(adapter: HardwareAdapter=None) -> handlers.HandlerCallbackDict:
    if adapter is None:
        adapter = MockedHardwareAdapter()
    return handlers.to_dict(HardwareHandler(adapter), ProcessHandler(adapter))


@pytest.fixture
def hardware_adapter():
    return MockedHardwareAdapter()


@async_context_manager
async def connect_dummy(ctx: zmq.asyncio.Context, dummy: Callable[[DealerRouterSocket], Awaitable[None]], *args,
                  endpoint: str='inproc://controller', client_class=AsyncClient, **kwargs):
    with DealerRouterSocket(ctx, zmq.ROUTER, side=ServerSide) as socket:
        socket.bind(endpoint)

        async def target():
            await dummy(socket, *args, **kwargs)

            # TODO
            # ident, msgs = await socket.recv_msgs()
            # _msgs = []  # type: List[Message]
            # _msgs.extend(motor.Action(port, motor.POWER, 0) for port in range(0, 4))
            # _msgs.extend(servo.Action(port, False, 0) for port in range(0, 4))
            # assert msgs == tuple(_msgs)
            # socket.send_msgs(ident, [ack.Acknowledgement()] * 8)

        task = asyncio.ensure_future(target())
        try:
            async with client_class(ctx, endpoint) as client:
                yield client
            await task
        finally:
            task.cancel()


@pytest.fixture
async def server(zmq_aio_ctx: zmq.asyncio.Context, hardware_adapter: HardwareAdapter):
    async with HedgehogServer(zmq_aio_ctx, 'inproc://controller', handler(hardware_adapter)) as server:
        yield 'inproc://controller'


@pytest.fixture
async def client(zmq_aio_ctx: zmq.asyncio.Context, server: str):
    async with AsyncClient(zmq_aio_ctx, server) as client:
        yield client


@pytest.mark.asyncio
async def test_concurrent_commands(client: AsyncClient, hardware_adapter: MockedHardwareAdapter):
    hardware_adapter.set_digital(8, 0, True)

    task_a = asyncio.ensure_future(client.get_analog(0))
    task_b = asyncio.ensure_future(client.get_digital(8))
    assert await task_a == 0
    assert await task_b is True


@pytest.mark.asyncio
@pytest.mark.parametrize('hardware_adapter', [HardwareAdapter()])
async def test_unsupported(client: AsyncClient):
    with pytest.raises(errors.UnsupportedCommandError):
        await client.get_analog(0)


class Commands(object):
    @staticmethod
    async def io_action_input(server, port, pullup):
        ident, msg = await server.recv_msg()
        assert msg == io.Action(port, io.INPUT_PULLUP if pullup else io.INPUT_FLOATING)
        await server.send_msg(ident, ack.Acknowledgement())

    @staticmethod
    async def io_command_request(server, port, flags):
        ident, msg = await server.recv_msg()
        assert msg == io.CommandRequest(port)
        await server.send_msg(ident, io.CommandReply(port, flags))

    @staticmethod
    async def analog_request(server, port, value):
        ident, msg = await server.recv_msg()
        assert msg == analog.Request(port)
        await server.send_msg(ident, analog.Reply(port, value))

    @staticmethod
    async def digital_request(server, port, value):
        ident, msg = await server.recv_msg()
        assert msg == digital.Request(port)
        await server.send_msg(ident, digital.Reply(port, value))

    @staticmethod
    async def io_action_output(server, port, level):
        ident, msg = await server.recv_msg()
        assert msg == io.Action(port, io.OUTPUT_ON if level else io.OUTPUT_OFF)
        await server.send_msg(ident, ack.Acknowledgement())

    @staticmethod
    async def motor_action(server, port, state, amount):
        ident, msg = await server.recv_msg()
        assert msg == motor.Action(port, state, amount)
        await server.send_msg(ident, ack.Acknowledgement())

    @staticmethod
    async def motor_command_request(server, port, state, amount):
        ident, msg = await server.recv_msg()
        assert msg == motor.CommandRequest(port)
        await server.send_msg(ident, motor.CommandReply(port, state, amount))

    @staticmethod
    async def motor_state_request(server, port, velocity, position):
        ident, msg = await server.recv_msg()
        assert msg == motor.StateRequest(port)
        await server.send_msg(ident, motor.StateReply(port, velocity, position))

    @staticmethod
    async def motor_set_position_action(server, port, position):
        ident, msg = await server.recv_msg()
        assert msg == motor.SetPositionAction(port, position)
        await server.send_msg(ident, ack.Acknowledgement())

    @staticmethod
    async def servo_action(server, port, active, position):
        ident, msg = await server.recv_msg()
        assert msg == servo.Action(port, active, position)
        await server.send_msg(ident, ack.Acknowledgement())

    @staticmethod
    async def servo_command_request(server, port, active, position):
        ident, msg = await server.recv_msg()
        assert msg == servo.CommandRequest(port)
        await server.send_msg(ident, servo.CommandReply(port, active, position))

    @staticmethod
    async def execute_process_echo_asdf(server, pid):
        ident, msg = await server.recv_msg()
        assert msg == process.ExecuteAction('echo', 'asdf')
        await server.send_msg(ident, process.ExecuteReply(pid))
        await server.send_msg(ident, process.StreamUpdate(pid, process.STDOUT, b'asdf\n'))
        await server.send_msg(ident, process.StreamUpdate(pid, process.STDOUT))
        await server.send_msg(ident, process.StreamUpdate(pid, process.STDERR))
        await server.send_msg(ident, process.ExitUpdate(pid, 0))

    @staticmethod
    async def execute_process_cat(server, pid):
        ident, msg = await server.recv_msg()
        assert msg == process.ExecuteAction('cat')
        await server.send_msg(ident, process.ExecuteReply(pid))

        while True:
            ident, msg = await server.recv_msg()
            chunk = msg.chunk
            assert msg == process.StreamAction(pid, process.STDIN, chunk)
            await server.send_msg(ident, ack.Acknowledgement())

            await server.send_msg(ident, process.StreamUpdate(pid, process.STDOUT, chunk))

            if chunk == b'':
                break

        await server.send_msg(ident, process.StreamUpdate(pid, process.STDERR))
        await server.send_msg(ident, process.ExitUpdate(pid, 0))


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
