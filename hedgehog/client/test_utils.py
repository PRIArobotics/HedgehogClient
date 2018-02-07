from typing import Awaitable, Callable, List

import pytest

import asyncio
import zmq.asyncio
from contextlib import contextmanager
from aiostream.context_utils import async_context_manager

from hedgehog.protocol import ServerSide
from hedgehog.protocol.async_sockets import DealerRouterSocket
from hedgehog.protocol.messages import Message, ack, analog, digital, io, motor, servo, process
from hedgehog.server import handlers, HedgehogServer
from hedgehog.server.handlers.hardware import HardwareHandler
from hedgehog.server.handlers.process import ProcessHandler
from hedgehog.server.hardware import HardwareAdapter
from hedgehog.server.hardware.mocked import MockedHardwareAdapter
from hedgehog.utils.event_loop import EventLoopThread


def handler(adapter: HardwareAdapter=None) -> handlers.HandlerCallbackDict:
    if adapter is None:
        adapter = MockedHardwareAdapter()
    return handlers.to_dict(HardwareHandler(adapter), ProcessHandler(adapter))


@pytest.fixture
def start_dummy(zmq_aio_ctx: zmq.asyncio.Context):
    @async_context_manager
    async def do_start(server_coro: Callable[[DealerRouterSocket], Awaitable[None]], *args,
                          endpoint: str='inproc://controller', **kwargs):
        with DealerRouterSocket(zmq_aio_ctx, zmq.ROUTER, side=ServerSide) as socket:
            socket.bind(endpoint)

            async def target():
                await server_coro(socket, *args, **kwargs)

                ident, msgs = await socket.recv_msgs()
                _msgs = []  # type: List[Message]
                _msgs.extend(motor.Action(port, motor.POWER, 0) for port in range(0, 4))
                _msgs.extend(servo.Action(port, False, 0) for port in range(0, 4))
                assert msgs == tuple(_msgs)
                await socket.send_msgs(ident, [ack.Acknowledgement()] * 8)

            task = asyncio.ensure_future(target())
            try:
                yield endpoint
                await task
            finally:
                task.cancel()

    return do_start


@pytest.fixture
def start_dummy_sync(start_dummy):
    @contextmanager
    def do_start(server_coro: Callable[[DealerRouterSocket], Awaitable[None]], *args,
                 endpoint: str='inproc://controller', **kwargs):
        with EventLoopThread() as looper, \
                looper.context(start_dummy(server_coro, *args, endpoint=endpoint, **kwargs)) as dummy:
            yield dummy

    return do_start


@pytest.fixture
def start_server(zmq_aio_ctx: zmq.asyncio.Context):
    @async_context_manager
    async def do_start(hardware_adapter: HardwareAdapter=None, endpoint: str='inproc://controller'):
        async with HedgehogServer(zmq_aio_ctx, endpoint, handler(hardware_adapter)):
            yield endpoint

    return do_start


@pytest.fixture
def start_server_sync(start_server):
    @contextmanager
    def do_start(hardware_adapter: HardwareAdapter=None, endpoint: str='inproc://controller'):
        with EventLoopThread() as looper, \
                looper.context(start_server(hardware_adapter=hardware_adapter, endpoint=endpoint)) as server:
            yield server

    return do_start


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

