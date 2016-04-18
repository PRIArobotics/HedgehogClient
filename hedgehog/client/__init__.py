import zmq
from hedgehog.protocol import messages, sockets


class HedgehogClient:
    def __init__(self, endpoint, context=None):
        context = context or zmq.Context.instance()
        socket = context.socket(zmq.DEALER)
        socket.connect(endpoint)
        self.socket = sockets.DealerWrapper(socket)

    def get_analogs(self, *ports):
        self.socket.send(messages.AnalogRequest(ports))
        sensors = self.socket.recv().analog_update.sensors
        return [sensors[port] for port in ports]

    def get_analog(self, port):
        return self.get_analogs(port)[0]

    def close(self):
        self.socket.close()
