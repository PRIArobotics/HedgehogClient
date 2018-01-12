from typing import Callable, Coroutine, Tuple, TypeVar

import concurrent.futures
import os
import signal
import sys
import threading
import time
import zmq.asyncio
from contextlib import contextmanager

from hedgehog.utils.event_loop import EventLoopThread
from hedgehog.protocol import errors
from hedgehog.protocol.messages import motor
from . import async_client

T = TypeVar('T')


class SyncClient(object):
    def __init__(self, ctx: zmq.asyncio.Context, endpoint: str='tcp://127.0.0.1:10789') -> None:
        self._loop = EventLoopThread()
        self.ctx = ctx
        self.endpoint = endpoint
        self.client = None  # type: async_client.AsyncClient

    def _create_client(self):
        return async_client.AsyncClient(self.ctx, self.endpoint)  # pragma: nocover

    def _call(self, coro: Coroutine[None, None, T]) -> T:
        return self._loop.run_coroutine(coro).result()

    def _enter(self, daemon=False):
        if self.client is None:
            async def create_client():
                return self._create_client()

            type(self._loop).__enter__(self._loop)
            try:
                self.client = self._loop.run_coroutine(create_client()).result()
                self._call(self.client._aenter(daemon=daemon))
                return self
            except:
                self.client = None
                if not type(self._loop).__exit__(self._loop, *sys.exc_info()):
                    raise
        else:
            self._call(self.client._aenter(daemon=daemon))
            return self

    def _exit(self, exc_type, exc_val, exc_tb, daemon=False):
        try:
            suppress = self._call(self.client._aexit(exc_type, exc_val, exc_tb, daemon=daemon))
        except:
            if not self.client.is_closed:
                raise
            if not type(self._loop).__exit__(self._loop, *sys.exc_info()):
                raise
        else:
            if not self.client.is_closed:
                return suppress

            if suppress:
                exc_type, exc_val, exc_tb = None, None, None
            return type(self._loop).__exit__(self._loop, exc_type, exc_val, exc_tb)

    def __enter__(self):
        return self._enter()

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._exit(exc_type, exc_val, exc_tb)

    @property
    @contextmanager
    def daemon(self):
        ret = self._enter(daemon=True)
        try:
            yield ret
        except:
            if not self._exit(*sys.exc_info(), daemon=True):
                raise
        else:
            self._exit(None, None, None, daemon=True)

    @property
    def is_shutdown(self):
        return self.client is not None and self.client.is_shutdown

    @property
    def is_closed(self):
        return self.client is not None and self.client.is_closed

    def _call_safe(self, coro_fun):
        if self.client is None or self.client.is_closed:
            raise RuntimeError("The client is not active, use `async with client:`")
        return self._call(coro_fun())

    def shutdown(self) -> None:
        self._call_safe(lambda: self.client.shutdown())

    def spawn(self, callback, *args, name=None, daemon=False, **kwargs) -> threading.Thread:
        future = concurrent.futures.Future()

        def target(*args, **kwargs):
            with (self.daemon if daemon else self):
                future.set_result(None)
                callback(*args, **kwargs)

        result = threading.Thread(target=target, name=name, args=args, kwargs=kwargs)
        result.start()
        future.result()
        return result


class HedgehogClientMixin(object):
    def _create_client(self):
        return async_client.HedgehogClient(self.ctx, self.endpoint)

    def set_input_state(self, port: int, pullup: bool) -> None:
        self._call_safe(lambda: self.client.set_input_state(port, pullup))

    def get_analog(self, port: int) -> int:
        return self._call_safe(lambda: self.client.get_analog(port))

    def get_digital(self, port: int) -> bool:
        return self._call_safe(lambda: self.client.get_digital(port))

    def set_digital_output(self, port: int, level: bool) -> None:
        self._call_safe(lambda: self.client.set_digital_output(port, level))

    def get_io_config(self, port: int) -> int:
        return self._call_safe(lambda: self.client.get_io_config(port))

    def set_motor(self, port: int, state: int, amount: int=0,
                  reached_state: int=motor.POWER, relative: int=None, absolute: int=None,
                  on_reached: Callable[[int, int], None]=None) -> None:
        self._call_safe(lambda: self.client.set_motor(port, state, amount, reached_state, relative, absolute, on_reached))

    def move(self, port: int, amount: int, state: int=motor.POWER) -> None:
        self._call_safe(lambda: self.client.move(port, amount, state))

    def move_relative_position(self, port: int, amount: int, relative: int, state: int=motor.POWER,
                               on_reached: Callable[[int, int], None]=None) -> None:
        self._call_safe(lambda: self.client.move_relative_position(port, amount, relative, state, on_reached))

    def move_absolute_position(self, port: int, amount: int, absolute: int, state: int=motor.POWER,
                               on_reached: Callable[[int, int], None]=None) -> None:
        self._call_safe(lambda: self.client.move_absolute_position(port, amount, absolute, state, on_reached))

    def get_motor_command(self, port: int) -> Tuple[int, int]:
        return self._call_safe(lambda: self.client.get_motor_command(port))

    def get_motor_state(self, port: int) -> Tuple[int, int]:
        return self._call_safe(lambda: self.client.get_motor_state(port))

    def get_motor_velocity(self, port: int) -> int:
        return self._call_safe(lambda: self.client.get_motor_velocity(port))

    def get_motor_position(self, port: int) -> int:
        return self._call_safe(lambda: self.client.get_motor_position(port))

    def set_motor_position(self, port: int, position: int) -> None:
        self._call_safe(lambda: self.client.set_motor_position(port, position))

    def set_servo(self, port: int, active: bool, position: int) -> None:
        self._call_safe(lambda: self.client.set_servo(port, active, position))

    def get_servo_command(self, port: int) -> Tuple[bool, int]:
        return self._call_safe(lambda: self.client.get_servo_command(port))

    def execute_process(self, *args: str, working_dir: str=None, on_stdout=None, on_stderr=None, on_exit=None) -> int:
        return self._call_safe(
            lambda: self.client.execute_process(*args, working_dir=working_dir,
                                                on_stdout=on_stdout, on_stderr=on_stderr, on_exit=on_exit))

    def signal_process(self, pid: int, signal: int=2) -> None:
        self._call_safe(lambda: self.client.signal_process(pid, signal))

    def send_process_data(self, pid: int, chunk: bytes=b'') -> None:
        self._call_safe(lambda: self.client.send_process_data(pid, chunk))


class HedgehogClient(HedgehogClientMixin, SyncClient):
    pass


@contextmanager
def connect(endpoint='tcp://127.0.0.1:10789', emergency=None,
            ctx=None, client_class=HedgehogClient, process_setup=True):
    # TODO SIGINT handling

    ctx = ctx or zmq.asyncio.Context()
    with client_class(ctx, endpoint) as client:
        # TODO a remote application's emergency_stop is remote, so it won't work in case of a disconnection!
        def emergency_stop():
            try:
                client.set_input_state(emergency, True)
                # while not client.get_digital(emergency):
                while client.get_digital(emergency):
                    time.sleep(0.1)

                os.kill(os.getpid(), signal.SIGINT)
            except errors.EmergencyShutdown:
                pass

        if emergency is not None:
            client.spawn(emergency_stop, name="emergency_stop", daemon=True)

        try:
            yield client
        except errors.EmergencyShutdown as ex:
            print(ex)
