from hedgehog.client.client_handle import ClientHandle


class ClientRegistry(object):
    def __init__(self):
        self.clients = {}

    def connect(self, key):
        client_handle = ClientHandle()
        self.clients[key] = client_handle
        return client_handle

    def disconnect(self, key):
        del self.clients[key]
