from contextlib import contextmanager
from pycreate2 import Create2
from .sync_client import HedgehogClient, connect


@contextmanager
def connect_create(port='/dev/ttyUSB0', baud=57600):
    create = Create2(port, baud=baud)
    create.start()
    yield create
    # TODO do proper cleanup instead of waiting for the GC
    # del create does not seem to work, and create.__del__() raises an exception upon actual GC


@contextmanager
def connect_create2(port='/dev/ttyUSB0', baud=115200):
    create = Create2(port, baud=baud)
    create.start()
    yield create
    # TODO do proper cleanup instead of waiting for the GC
