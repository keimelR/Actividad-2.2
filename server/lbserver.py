import key_value_store_service_pb2
import key_value_store_service_pb2_grpc

from concurrent import futures
import os
import threading
import grpc
import datetime

class KeyValueServer(key_value_store_service_pb2_grpc.KeyValueStoreServicer):
    def __init__(self):
        self.data = {}
        self.file = open("./server/database.log", "ab")
        self.lock = threading.Lock()
        
        self.time_started = 0
        self.total_requests = 0
        self.total_set_requests = 0
        self.total_get_requests = 0
        self.total_get_prefix_requests = 0
        
        self.recover_data()
        
    def recover_data(self):
        try:
            with open("./server/database.log", "rb") as file:
                for line in file:
                    key, value = line.strip().split(":", 1)
                    self.data[key] = value
        except FileNotFoundError:
            pass
        
    def Get(self, request, context):
        with self.lock:  
            value = self.data.get(request.key)
            
            if value is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                response = key_value_store_service_pb2.GetValueResponse(status = False, value = f"Clave no encontrada")
                return response
                
            response = key_value_store_service_pb2.GetValueResponse(status = True, value = value)

            self.total_requests += 1
            self.total_get_requests += 1
            
            print(f"Se ha recibido una peticion de Get")
            return response
    
    # Crear condicion para que no se pueda escribir un archivo con una llave ya existente, si la llave existe se debe actualizar el valor
    def Set(self, request, context):
        with self.lock:
            key = request.key
            value = request.value
            
            # Medida para evitar claves duplicadas
            if key in self.data:
                context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                response = key_value_store_service_pb2.SetKeyValueResponse(status = False, message = "Se ha detectado una clave ya existente")
                return response
               
            # Log the operation to the file          
            self.file.write(f"{key}:{value}\n".encode("utf-8"))
            self.file.flush()
            os.fsync(self.file.fileno())
                
            self.data[key] = value
            
            self.total_set_requests += 1
            self.total_requests += 1
            
            print("Se ha recibido una peticion Set")
            response = key_value_store_service_pb2.SetKeyValueResponse(status = True, message = f"Set: {request.key} = {request.value}")
            return response
    
    def GetPrefixKey(self, request, context):
        with self.lock:
            keys = []
            values = []
            for key, value in self.data.items():
                if key.startswith(request.prefixKey):
                    keys.append(key)
                    values.append(value)
            
            response = key_value_store_service_pb2.GetPrefixResponse(keys = keys, values = values)
            
            self.total_get_prefix_requests += 1
            self.total_requests += 1
            
            print("Se ha recibido una peticion getPrefix")
            return response
    
    def Stat(self, request, context):
        with self.lock:
            response = key_value_store_service_pb2.StatResponse(
                time_started = datetime.datetime.now().isoformat(),
                total_requests = self.total_requests,
                total_set_requests = self.total_set_requests,
                total_get_requests = self.total_get_requests,
                total_get_prefix_requests = self.total_get_prefix_requests
            )
            print("Se ha recibido una peticion Stat")
            return response
    
    
def main():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    key_value_store_service_pb2_grpc.add_KeyValueStoreServicer_to_server(KeyValueServer(), server)
    
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