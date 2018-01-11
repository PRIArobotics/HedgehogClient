from typing import Coroutine, TypeVar

import concurrent.futures
import sys
import threading
import zmq.asyncio
from contextlib import contextmanager

from hedgehog.utils.event_loop import EventLoopThread
from . import async_client

T = TypeVar('T')


class SyncClient(object):
    def __init__(self, ctx: zmq.asyncio.Context, endpoint: str='tcp://127.0.0.1:10789') -> None:
        self._loop = EventLoopThread()
        self.ctx = ctx
        self.endpoint = endpoint
        self.client = None  # type: AsyncClient

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


class HedgehogClient(HedgehogClientMixin, SyncClient):
    pass
