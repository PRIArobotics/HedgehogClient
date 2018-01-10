from typing import cast, Any, Awaitable, Callable, List, Optional, Sequence, Tuple

import asyncio
import logging
import sys
import zmq.asyncio
from aiostream.context_utils import async_context_manager
from hedgehog.utils.asyncio import Actor
from hedgehog.protocol import errors, ClientSide
from hedgehog.protocol.async_sockets import DealerRouterSocket
from hedgehog.protocol.messages import Message, ack, io, analog, digital, motor, servo, process
from .async_handlers import EventHandler, HandlerRegistry, process_handler

logger = logging.getLogger(__name__)


class AsyncClient(Actor):
    _SHUTDOWN = object()

    def __init__(self, ctx: zmq.asyncio.Context, endpoint: str='tcp://127.0.0.1:10789') -> None:
        super(AsyncClient, self).__init__()
        self.ctx = ctx
        self.endpoint = endpoint
        self.registry = HandlerRegistry()
        self.socket = None  # type: DealerRouterSocket
        self._commands = asyncio.Queue()
        self._futures = []  # type: List[Tuple[Sequence[Optional[EventHandler]], asyncio.Future]]

        self._open_count = 0
        self._daemon_count = 0
        self._shutdown = False

    async def _aenter(self, daemon=False):
        if daemon and self._open_count == 0:
            raise RuntimeError("The client is not active, first use of the client must not be daemon")
        if self._shutdown:
            raise RuntimeError("Cannot reuse a client after it was once shut down")

        self._open_count += 1
        if daemon:
            self._daemon_count += 1

        if self._open_count == 1:
            await super(AsyncClient, self).__aenter__()
        return self

    async def _aexit(self, exc_type, exc_val, exc_tb, daemon=False):
        if daemon:
            self._daemon_count -= 1

        if self._open_count - 1 == self._daemon_count:
            await self.shutdown()
        if self._open_count == 1:
            await super(AsyncClient, self).__aexit__(exc_type, exc_val, exc_tb)
        self._open_count -= 1

    async def __aenter__(self):
        return await self._aenter()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._aexit(exc_type, exc_val, exc_tb)

    @property
    @async_context_manager
    async def daemon(self):
        ret = await self._aenter(daemon=True)
        try:
            yield ret
        except:
            if not await self._aexit(*sys.exc_info(), daemon=True):
                raise
        else:
            await self._aexit(None, None, None, daemon=True)

    @property
    def is_shutdown(self):
        return self._shutdown

    @property
    def is_closed(self):
        return self.is_shutdown and self._open_count == 0

    async def _handle_commands(self):
        while True:
            cmds, future = await self._commands.get()
            if cmds is AsyncClient._SHUTDOWN:
                if self._shutdown:
                    future.set_result(None)
                else:
                    self._shutdown = True
                    self.registry.shutdown()

                    msgs = []  # type: List[Message]
                    msgs.extend(motor.Action(port, motor.POWER, 0) for port in range(0, 4))
                    msgs.extend(servo.Action(port, False, 0) for port in range(0, 4))
                    self._futures.append((tuple(None for _ in msgs), future))
                    await self.socket.send_msgs((), msgs)
            elif self._shutdown:
                replies = [ack.Acknowledgement(ack.FAILED_COMMAND, "Emergency Shutdown activated") for _ in cmds]
                future.set_result(replies)
                for _, handler in cmds:
                    if handler is not None:
                        handler.close()
            else:
                self._futures.append((tuple(handler for _, handler in cmds), future))
                await self.socket.send_msgs((), tuple(msg for msg, _ in cmds))

    async def _handle_updates(self):
        while True:
            _, msgs = await self.socket.recv_msgs()
            assert len(msgs) > 0

            # either, all messages are replies corresponding to the previous requests,
            # or all messages are asynchronous updates
            if msgs[0].is_async:
                # handle asynchronous messages
                self.registry.handle_async(msgs)
            else:
                # handle synchronous messages
                handlers, future = self._futures.pop(0)
                future.set_result(msgs)
                self.registry.register(handlers, msgs)

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
                try:
                    await commands
                except asyncio.CancelledError:
                    pass
                updates.cancel()
                try:
                    await updates
                except asyncio.CancelledError:
                    pass

    async def spawn(self, awaitable: Awaitable[Any], daemon: bool=False) -> asyncio.Task:
        future = asyncio.Future()

        async def task():
            async with (self.daemon if daemon else self):
                future.set_result(None)
                await awaitable

        result = asyncio.ensure_future(task())
        await future
        return result

    async def send(self, msg: Message, handler: EventHandler=None) -> Optional[Message]:
        reply, = await self.send_multipart((msg, handler))
        if isinstance(reply, ack.Acknowledgement):
            if reply.code != ack.OK:
                raise errors.error(reply.code, reply.message)
            return None
        else:
            return reply

    async def send_multipart(self, *cmds: Tuple[Message, EventHandler]) -> Any:
        if self._open_count == 0:
            raise RuntimeError("The client is not active, use `async with client:`")
        future = asyncio.Future()
        await self._commands.put((cmds, future))
        return await future

    async def shutdown(self) -> None:
        if self._open_count == 0:
            raise RuntimeError("The client is not active, use `async with client:`")
        future = asyncio.Future()
        await self._commands.put((AsyncClient._SHUTDOWN, future))
        await future

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
        response = cast(process.ExecuteReply, await self.send(process.ExecuteAction(*args, working_dir=working_dir), handler))
        return response.pid

    async def signal_process(self, pid: int, signal: int=2) -> None:
        await self.send(process.SignalAction(pid, signal))

    async def send_process_data(self, pid: int, chunk: bytes=b'') -> None:
        await self.send(process.StreamAction(pid, process.STDIN, chunk))
