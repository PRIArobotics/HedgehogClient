import asyncio
import logging
import signal
import threading
from collections import namedtuple, OrderedDict
from contextlib import contextmanager

logger = logging.getLogger(__name__)


__Handler = namedtuple('__Handler', ('old_handler', 'callbacks'))

__handlers = {}
__async_handlers = {}


@contextmanager
def register(signalnum, callback):
    if threading.current_thread() is not threading.main_thread():
        raise RuntimeError("Signal handlers can only be manipulated from the main thread")

    if signalnum in __async_handlers:
        raise RuntimeError("Can't combine regular and event loop signal handling")

    if signalnum not in __handlers:
        old_handler = signal.getsignal(signalnum)
        if old_handler is None:
            # None means that the previous signal handler was not installed from Python
            # it's not legal to pass None to signal(), so restore the default
            logger.warning("Removing a signal handler that can't be restored")
            if signalnum == signal.SIGINT:
                old_handler = signal.default_int_handler
            else:
                old_handler = signal.SIG_DFL

        handler = __handlers[signalnum] = __Handler(old_handler, OrderedDict())

        if callable(old_handler) and old_handler is not signal.default_int_handler:
            # the old signal handler is a callable; add it to our wrapper handler
            # to retain its behavior.
            handler.callbacks[None] = old_handler

        def do_handle(signalnum, frame):
            for key, callback in reversed(__handlers[signalnum].callbacks.items()):
                try:
                    callback(signalnum, frame)
                except Exception:
                    if key is None:
                        # this is the old signal handler; if we didn't install our own, this would not be caught,
                        # so reraise it here
                        raise
                    logger.exception("Exception in signal handler")

        signal.signal(signalnum, do_handle)
    else:
        handler = __handlers[signalnum]

    # add the callback to the end, but maintain a unique key to remove the callback later
    key = object()
    handler.callbacks[key] = callback
    try:
        yield
    finally:
        del handler.callbacks[key]
        if len(handler.callbacks) == 0 or (len(handler.callbacks) == 1 and None in handler.callbacks):
            # there are no more handlers, or just the old handler
            signal.signal(signalnum, handler.old_handler)
            del __handlers[signalnum]


@contextmanager
def register_async(signalnum, callback):
    if threading.current_thread() is not threading.main_thread():
        raise RuntimeError("Signal handlers can only be manipulated from the main thread")

    if signalnum in __handlers:
        raise RuntimeError("Can't combine regular and event loop signal handling")

    loop = asyncio.get_event_loop()

    if signalnum not in __async_handlers:
        old_handler = signal.getsignal(signalnum)
        if old_handler is None:
            # None means that the previous signal handler was not installed from Python
            # it's not legal to pass None to signal(), so restore the default
            logger.warning("Removing a signal handler that can't be restored")
            if signalnum == signal.SIGINT:
                old_handler = signal.default_int_handler
            else:
                old_handler = signal.SIG_DFL

        handler = __async_handlers[signalnum] = __Handler(old_handler, OrderedDict())

        def do_handle():
            for callback in reversed(__async_handlers[signalnum].callbacks.values()):
                try:
                    callback()
                except Exception:
                    logger.exception("Exception in signal handler")

        loop.add_signal_handler(signalnum, do_handle)
    else:
        handler = __async_handlers[signalnum]

    # add the callback to the end, but maintain a unique key to remove the callback later
    key = object()
    handler.callbacks[key] = callback
    try:
        yield
    finally:
        del handler.callbacks[key]
        if len(handler.callbacks) == 0:
            # there are no more handlers
            loop.remove_signal_handler(signalnum)
            signal.signal(signalnum, handler.old_handler)
            del __async_handlers[signalnum]
