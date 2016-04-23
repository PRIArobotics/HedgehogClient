import zmq
from hedgehog.protocol import sockets
from hedgehog.protocol.messages import analog


class HedgehogClient:
    def __init__(self, endpoint, context=None):
        context = context or zmq.Context.instance()
        socket = context.socket(zmq.DEALER)
        socket.connect(endpoint)
        self.socket = sockets.DealerWrapper(socket)

    def get_analog(self, port):
        self.socket.send(analog.Request(port))
        response = self.socket.recv()
        assert response.port == port
        return response.value

    def close(self):
        self.socket.close()
