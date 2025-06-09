from protobuf import key_value_store_service_pb2
from protobuf import key_value_store_service_pb2_grpc
from concurrent import futures
import grpc
    
def main():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = key_value_store_service_pb2_grpc.KeyValueStoreServiceStub(channel)
        