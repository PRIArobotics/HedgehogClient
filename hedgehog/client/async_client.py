from typing import cast, Any, AsyncIterator, Callable, List, Optional, Sequence, Tuple

import asyncio
import logging
import os
import signal
import sys
import time
import zmq.asyncio
from contextlib import contextmanager, suppress
from hedgehog.utils.asyncio import Actor, stream_from_queue
from hedgehog.protocol import errors, ClientSide
from hedgehog.protocol.async_sockets import ReqSocket, DealerRouterSocket
from hedgehog.protocol.messages import Message, ack, io, analog, digital, motor, servo, process
from pycreate2 import Create2
from .client_backend import ClientBackend
from .client_registry import EventHandler, process_handler

logger = logging.getLogger(__name__)


class AsyncClient(Actor):
    def __init__(self, ctx: zmq.asyncio.Context, endpoint: str='tcp://127.0.0.1:10789') -> None:
        super(AsyncClient, self).__init__()
        self.ctx = ctx
        self.endpoint = endpoint
        self.socket = None  # type: DealerRouterSocket
        self._commands = asyncio.Queue()
        self._futures = []  # type: List[asyncio.Future]

    async def _handle_commands(self):
        while True:
            msgs, future = await self._commands.get()
            self._futures.append(future)
            await self.socket.send_msgs((), msgs)

    async def _handle_updates(self):
        while True:
            _, msgs = await self.socket.recv_msgs()
            assert len(msgs) > 0

            # either, all messages are replies corresponding to the previous requests,
            # or all messages are asynchronous updates
            if msgs[0].is_async:
                # handle asynchronous messages
                for msg in msgs:
                    # TODO do any kind of update handling
                    pass
            else:
                # handle synchronous messages
                future = self._futures.pop(0)
                future.set_result(msgs)
                # TODO register any kind of update handling

    async def run(self, cmd_pipe, evt_pipe) -> None:
        # TODO having to explicitly use empty headers is ugly
        with DealerRouterSocket(self.ctx, zmq.DEALER, side=ClientSide) as self.socket:
            self.socket.connect(self.endpoint)
            await evt_pipe.send(b'$START')

            commands = asyncio.ensure_future(self._handle_commands())
            updates = asyncio.ensure_future(self._handle_updates())
            try:
                while True:
                    cmd = await cmd_pipe.recv()
                    if cmd == b'$TERM':
                        break
            finally:
                commands.cancel()
                updates.cancel()

    async def send(self, msg: Message) -> Optional[Message]:
        reply, = await self.send_multipart(msg)
        if isinstance(reply, ack.Acknowledgement):
            if reply.code != ack.OK:
                raise errors.error(reply.code, reply.message)
            return None
        else:
            return reply

    async def send_multipart(self, *msgs: Message) -> Any:
        future = asyncio.Future()
        await self._commands.put((msgs, future))
        return await future

    async def set_input_state(self, port: int, pullup: bool) -> None:
        await self.send(io.Action(port, io.INPUT_PULLUP if pullup else io.INPUT_FLOATING))

    async def get_analog(self, port: int) -> int:
        response = cast(analog.Reply, await self.send(analog.Request(port)))
        assert response.port == port
        return response.value

    async def get_digital(self, port: int) -> bool:
        response = cast(digital.Reply, await self.send(digital.Request(port)))
        assert response.port == port
        return response.value

    async def set_digital_output(self, port: int, level: bool) -> None:
        await self.send(io.Action(port, io.OUTPUT_ON if level else io.OUTPUT_OFF))

    async def get_io_config(self, port: int) -> int:
        response = cast(io.CommandReply, await self.send(io.CommandRequest(port)))
        assert response.port == port
        return response.flags

    async def set_motor(self, port: int, state: int, amount: int=0,
                  reached_state: int=motor.POWER, relative: int=None, absolute: int=None,
                  on_reached: Callable[[int, int], None]=None) -> None:
        # if on_reached is not None:
        #     if relative is None and absolute is None:
        #         raise ValueError("callback given, but no end position")
        #     handler = MotorUpdateHandler(on_reached)
        # else:
        #     handler = None
        await self.send(motor.Action(port, state, amount, reached_state, relative, absolute))

    async def move(self, port: int, amount: int, state: int=motor.POWER) -> None:
        await self.set_motor(port, state, amount)

    async def move_relative_position(self, port: int, amount: int, relative: int, state: int=motor.POWER,
                               on_reached: Callable[[int, int], None]=None) -> None:
        await self.set_motor(port, state, amount, relative=relative, on_reached=on_reached)

    async def move_absolute_position(self, port: int, amount: int, absolute: int, state: int=motor.POWER,
                               on_reached: Callable[[int, int], None]=None) -> None:
        await self.set_motor(port, state, amount, absolute=absolute, on_reached=on_reached)

    async def get_motor_command(self, port: int) -> Tuple[int, int]:
        response = cast(motor.CommandReply, await self.send(motor.CommandRequest(port)))
        assert response.port == port
        return response.state, response.amount

    async def get_motor_state(self, port: int) -> Tuple[int, int]:
        response = cast(motor.StateReply, await self.send(motor.StateRequest(port)))
        assert response.port == port
        return response.velocity, response.position

    async def get_motor_velocity(self, port: int) -> int:
        velocity, _ = await self.get_motor_state(port)
        return velocity

    async def get_motor_position(self, port: int) -> int:
        _, position = await self.get_motor_state(port)
        return position

    async def set_motor_position(self, port: int, position: int) -> None:
        await self.send(motor.SetPositionAction(port, position))

    async def set_servo(self, port: int, active: bool, position: int) -> None:
        await self.send(servo.Action(port, active, position))

    async def get_servo_command(self, port: int) -> Tuple[bool, int]:
        response = cast(servo.CommandReply, await self.send(servo.CommandRequest(port)))
        assert response.port == port
        return response.active, response.position

    async def execute_process(self, *args: str, working_dir: str=None, on_stdout=None, on_stderr=None, on_exit=None) -> int:
        if on_stdout is not None or on_stderr is not None or on_exit is not None:
            handler = process_handler(on_stdout, on_stderr, on_exit)
        else:
            handler = None
        response = cast(process.ExecuteReply, await self.send(process.ExecuteAction(*args, working_dir=working_dir)))
        return response.pid

    async def signal_process(self, pid: int, signal: int=2) -> None:
        await self.send(process.SignalAction(pid, signal))

    async def send_process_data(self, pid: int, chunk: bytes=b'') -> None:
        await self.send(process.StreamAction(pid, process.STDIN, chunk))
