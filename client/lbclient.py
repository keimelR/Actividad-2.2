import grpc
import key_value_store_service_pb2 as pb2
import key_value_store_service_pb2_grpc as pb2_grpc
import random
import string

class KVClient:
    def __init__(self, address="localhost:50051"):
        options = [
            ("grpc.max_send_message_length", 6 * 1024 * 1024),      # 6 MB
            ("grpc.max_receive_message_length", 6 * 1024 * 1024)    # 6 MB
        ]
        self.channel = grpc.insecure_channel(address, options=options)
        self.stub = pb2_grpc.KeyValueStoreStub(self.channel)

    def get(self, key):
        request = pb2.GetValue(key=key)
        return self.stub.Get(request)

    def set(self, key, value):
        request = pb2.SetKeyValue(key=key, value=value)
        return self.stub.Set(request)

    def get_prefix(self, prefix):
        request = pb2.GetPrefix(prefixKey=prefix)
        return self.stub.GetPrefixKey(request)

    def stat(self):
        request = pb2.StatRequest()
        return self.stub.Stat(request)

    def close(self):
        self.channel.close()

def generate_value(size):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))
