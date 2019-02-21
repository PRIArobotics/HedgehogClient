from typing import cast, Any, Awaitable, Callable, Deque, List, Optional, Sequence, Tuple

import asyncio
import logging
import os
import signal
import threading
import zmq.asyncio
from collections import deque
from contextlib import asynccontextmanager, AsyncExitStack
from functools import partial

from concurrent_utils.pipe import PipeEnd
from concurrent_utils.component import Component, component_coro_wrapper, start_component
from hedgehog.protocol import errors, ClientSide
from hedgehog.protocol.zmq.asyncio import DealerRouterSocket
from hedgehog.protocol.messages import Message, ack, io, analog, digital, motor, servo, imu, process, speaker
from . import shutdown_handler
from .async_handlers import AsyncHandler, HandlerRegistry, ProcessHandler

logger = logging.getLogger(__name__)


class AsyncClient:
    def __init__(self, ctx: zmq.asyncio.Context, endpoint: str='tcp://127.0.0.1:10789') -> None:
        self.ctx = ctx
        self.endpoint = endpoint
        self.registry = HandlerRegistry()
        self.socket = None  # type: DealerRouterSocket
        self._reply_condition = asyncio.Condition()
        self._replies: Deque[Sequence[Message]] = deque()

        self._open_count = 0
        self._daemon_count = 0
        self._exit_stack = None  # type: AsyncExitStack
        self._shutdown = False

    async def _aenter(self, daemon=False):
        async with AsyncExitStack() as enter_stack:
            if daemon and self._open_count == 0:
                raise RuntimeError("The client is not active, first use of the client must not be daemon")
            if self._shutdown:
                raise RuntimeError("Cannot reuse a client after it was once shut down")

            logger.debug("Entering as %s", "daemon" if daemon else "regular")

            self._open_count += 1

            @enter_stack.callback
            def decrement_open_count():
                self._open_count -= 1

            if daemon:
                self._daemon_count += 1

                # testing: I see no good way to cause a fault that triggers this...
                # the code is almost the same as decrement_open_count, so ignore it for coverage
                @enter_stack.callback
                def decrement_daemon_count():
                    self._daemon_count -= 1  # pragma: nocover

            if self._open_count == 1:
                logger.debug("Activating client...")

                async with AsyncExitStack() as stack:
                    @asynccontextmanager
                    async def start() -> Component[None]:
                        component = await start_component(self.workload)
                        try:
                            yield component
                        finally:
                            await component.stop()
                    await stack.enter_async_context(start())

                    if threading.current_thread() is threading.main_thread():
                        loop = asyncio.get_event_loop()

                        def sigint_handler():
                            task = loop.create_task(self.shutdown())

                            # if this signal handler is called during _aenter, register the await with `stack`;
                            # otherwise, with `self._exit_stack`
                            exit_stack = self._exit_stack if self._exit_stack is not None else stack

                            @exit_stack.push_async_callback
                            async def await_shutdown():
                                await task

                        stack.enter_context(shutdown_handler.register_async(signal.SIGINT, sigint_handler))

                    # save the exit actions that need undoing...
                    self._exit_stack = stack.pop_all()

            # ...and discard those that were only for the error case
            enter_stack.pop_all()
            logger.debug("Open: %d (%d daemon)", self._open_count, self._daemon_count)
            return self

    async def _aexit(self, exc_type, exc_val, exc_tb, daemon=False):
        stack = AsyncExitStack()

        # called last
        @stack.callback
        def decrement_open_count():
            self._open_count -= 1
            logger.debug("Open: %d (%d daemon)", self._open_count, self._daemon_count)

        @stack.push_async_exit
        async def exit(exc_type, exc_val, exc_tb):
            if self._open_count == 1:
                logger.debug("Deactivating client...")
                try:
                    return await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)
                finally:
                    self._exit_stack = None

        @stack.push_async_callback
        async def shutdown():
            if self._open_count - 1 == self._daemon_count:
                await self.shutdown()

        @stack.callback
        def decrement_daemon_count():
            if daemon:
                self._daemon_count -= 1

        # called first
        @stack.push
        def suppress_shutdown_error(exc_type, exc_val, exc_tb):
            if exc_type == errors.EmergencyShutdown:
                print(exc_val)
                return True

        logger.debug("Exiting as %s", "daemon" if daemon else "regular")

        return await stack.__aexit__(exc_type, exc_val, exc_tb)

    async def __aenter__(self):
        return await self._aenter()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._aexit(exc_type, exc_val, exc_tb)

    @property
    @asynccontextmanager
    async def daemon(self):
        async with AsyncExitStack() as stack:
            ret = await self._aenter(daemon=True)
            stack.push_async_exit(partial(self._aexit, daemon=True))
            yield ret

    @property
    def is_shutdown(self):
        return self._shutdown

    @property
    def is_closed(self):
        return self.is_shutdown and self._open_count == 0

    @property
    @asynccontextmanager
    async def _job(self):
        if self._open_count == 0:
            raise RuntimeError("The client is not active, use `async with client:`")

        async with self._reply_condition:
            yield

    async def _handle_updates(self):
        while True:
            _, msgs = await self.socket.recv_msgs()
            assert len(msgs) > 0
            async with self._reply_condition:
                # either, all messages are replies corresponding to the previous requests,
                # or all messages are asynchronous updates
                if msgs[0].is_async:
                    assert all(msg.is_async for msg in msgs)

                    # handle asynchronous messages
                    logger.debug("Receive updates: %s", msgs)
                    self.registry.handle_updates(msgs)
                else:
                    assert not any(msg.is_async for msg in msgs)

                    # handle synchronous messages
                    self.registry.complete_register(msgs)
                    self._replies.append(msgs)
                    self._reply_condition.notify()

    async def _workload(self, *, commands: PipeEnd, events: PipeEnd) -> None:
        with DealerRouterSocket(self.ctx, zmq.DEALER, side=ClientSide) as self.socket:
            self.socket.connect(self.endpoint)
            await events.send(Component.EVENT_START)

            updates = asyncio.create_task(self._handle_updates())
            try:
                while True:
                    command = await commands.recv()
                    if command == Component.COMMAND_STOP:
                        break
                    else:
                        raise ValueError(f"unknown command: {command!r}")
            finally:
                updates.cancel()
                try:
                    await updates
                except asyncio.CancelledError:
                    pass

    async def workload(self, *, commands: PipeEnd, events: PipeEnd) -> None:
        return await component_coro_wrapper(self._workload, commands=commands, events=events)

    async def send(self, msg: Message, handler: AsyncHandler=None) -> Optional[Message]:
        reply, = await self.send_multipart((msg, handler))
        if isinstance(reply, ack.Acknowledgement):
            if reply.code != ack.OK:
                raise errors.error(reply.code, reply.message)
            return None
        else:
            return reply

    async def send_multipart(self, *cmds: Tuple[Message, AsyncHandler]) -> Any:
        async with self._job:
            if self._shutdown:
                replies = [ack.Acknowledgement(ack.FAILED_COMMAND, "Emergency Shutdown activated") for _ in cmds]
                for _, handler in cmds:
                    if handler is not None:
                        handler.close()
                return replies
            else:
                return await self._send(tuple(request for request, _ in cmds), tuple(handler for _, handler in cmds))

    async def shutdown(self) -> None:
        async with self._job:
            if not self._shutdown:
                self._shutdown = True
                self.registry.shutdown()

                msgs = []  # type: List[Message]
                msgs.extend(motor.Action(port, motor.POWER, 0) for port in range(0, 4))
                msgs.extend(servo.Action(port, False, 0) for port in range(0, 4))
                await self._send(msgs, tuple(None for _ in msgs))

    async def _send(self, requests: Sequence[Message], handlers: Sequence[AsyncHandler]) -> Sequence[Message]:
        logger.debug("Send commands:   %s", requests)
        self.registry.prepare_register(handlers)
        await self.socket.send_msgs((), requests)

        await self._reply_condition.wait()
        replies = self._replies.popleft()
        logger.debug("Receive replies: %s", replies)
        return replies

    async def spawn(self, awaitable: Awaitable[Any], daemon: bool=False) -> asyncio.Task:
        event = asyncio.Event()

        async def task():
            async with (self.daemon if daemon else self):
                event.set()
                await awaitable

        result = asyncio.create_task(task())
        await event.wait()
        return result


class HedgehogClientMixin:
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

    async def configure_motor(self, port: int, config: motor.Config) -> int:
        await self.send(motor.ConfigAction(port, config))

    async def configure_motor_dc(self, port: int) -> int:
        await self.configure_motor(port, motor.DcConfig())

    async def configure_motor_encoder(self, port: int, encoder_a_port: int, encoder_b_port: int) -> int:
        await self.configure_motor(port, motor.EncoderConfig(encoder_a_port, encoder_b_port))

    async def configure_motor_stepper(self, port: int) -> int:
        await self.configure_motor(port, motor.StepperConfig())

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

    async def get_imu_rate(self) -> Tuple[int, int, int]:
        response = cast(imu.RateReply, await self.send(imu.RateRequest()))
        return response.x, response.y, response.z

    async def get_imu_acceleration(self) -> Tuple[int, int, int]:
        response = cast(imu.AccelerationReply, await self.send(imu.AccelerationRequest()))
        return response.x, response.y, response.z

    async def get_imu_pose(self) -> Tuple[int, int, int]:
        response = cast(imu.PoseReply, await self.send(imu.PoseRequest()))
        return response.x, response.y, response.z

    async def execute_process(self, *args: str, working_dir: str=None, on_stdout=None, on_stderr=None, on_exit=None) -> int:
        if on_stdout is not None or on_stderr is not None or on_exit is not None:
            handler = ProcessHandler(on_stdout, on_stderr, on_exit)
        else:
            handler = None
        response = cast(process.ExecuteReply, await self.send(process.ExecuteAction(*args, working_dir=working_dir), handler))
        return response.pid

    async def signal_process(self, pid: int, signal: int=2) -> None:
        await self.send(process.SignalAction(pid, signal))

    async def send_process_data(self, pid: int, chunk: bytes=b'') -> None:
        await self.send(process.StreamAction(pid, process.STDIN, chunk))

    async def set_speaker(self, frequency: Optional[int]) -> None:
        await self.send(speaker.Action(frequency))


class HedgehogClient(HedgehogClientMixin, AsyncClient):
    pass


@asynccontextmanager
async def connect(endpoint='tcp://127.0.0.1:10789', emergency=None,
                  ctx=None, client_class=HedgehogClient, process_setup=True):
    # TODO SIGINT handling

    ctx = ctx or zmq.asyncio.Context()
    async with client_class(ctx, endpoint) as client:
        # TODO a remote application's emergency_stop is remote, so it won't work in case of a disconnection!
        async def emergency_stop():
            await client.set_input_state(emergency, True)
            # while not client.get_digital(emergency):
            while await client.get_digital(emergency):
                await asyncio.sleep(0.1)

            os.kill(os.getpid(), signal.SIGINT)

        if emergency is not None:
            await client.spawn(emergency_stop(), daemon=True)

        yield client
