from key_value_store_service_pb2 import (
    SetKeyValueResponse,
    GetValueResponse,
    GetPrefixResponse,
)

from key_value_store_service_pb2_grpc import (
    KeyValueStoreServicer,
    add_KeyValueStoreServicer_to_server
)

from concurrent import futures
import os
import threading
import grpc
    
class KeyValueStoreService(KeyValueStoreServicer):
    def __init__(self):
        self.data = {}
        self.file = open("database.log", "a")
        self.lock = threading.Lock()
        self.recover_data()
        
    def recover_data(self):
        try:
            with open("database.log", "r") as file:
                for line in file:
                    key, value = line.strip().split(":", 1)
                    self.data[key] = value
        except FileNotFoundError:
            pass
        
    def Get(self, request, context):
        with self.lock:
            key = request.key
            value = self.data.get(key, "")
            return GetValueResponse(value=value)
    
    def Set(self, request, context):
        with self.lock:
            key = request.key
            value = request.value
            self.data[key] = value
            
            # Log the operation to the file
            self.file.write(f"{key}:{value}\n")
            self.file.flush()
            os.fsync(self.file.fileno())
        
            return SetKeyValueResponse(success=True)
    
    def GetPrefix(self, request, context):
        with self.lock:
            prefix = request.prefix
            
            keys = []
            values = []
            
            for key, value in self.data.items():
                if key.startswith(prefix):
                    keys.append(key)
                    values.append(value)
            return GetPrefixResponse(keys=keys, values=values)
    
def main():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_KeyValueStoreServicer_to_server(KeyValueStoreService(), server)
    
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server started on port 50051")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("Server shutting down...")
        server.stop(0)
        
if __name__ == "__main__":
    main()