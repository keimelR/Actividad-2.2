import key_value_store_service_pb2
import key_value_store_service_pb2_grpc
from concurrent import futures
import grpc
    
    
def main():
    with grpc.insecure_channel('localhost:50051') as channel:
        print("Conectando al servidor gRPC")
        stub = key_value_store_service_pb2_grpc.KeyValueStoreStub(channel)
        """
        try: 
            request = key_value_store_service_pb2.GetPrefix(prefixKey = "2")
            result = stub.GetPrefixKey(request)
            
            print(f"Claves: {result.keys}\nValores: {result.values}")
        except grpc.RpcError as e:
            print(f"Error al intentar ejecutar GetPrefix")
            print(f"StatusCode: {e.code()}")
            print(f"Detalles del error: {e.details()}")
        except Exception as e:
            print(e)
        """
            
        """
        try:
            request = key_value_store_service_pb2.SetKeyValue(key = "4", value = "Prueba Cuarta")
            result = stub.Set(request)
            print(f"status = {(result.status)}\nmessage = {result.message}")
        except grpc.RpcError as e:
            print(f"Error al intentar ejecutar Set")
            print(f"StatusCode: {e.code()}")
            print(f"Detalles del error: {e.details()}")
        except Exception as e:
            print(e)
        """
        
        """   
        try:
            request = key_value_store_service_pb2.GetValue(key = '2')
            result = stub.Get(request)
            print(f"GRPC received: {result}")

        except grpc.RpcError as e:
            print(f"Error al intentar ejecutar Get")
            print(f"StatusCode: {e.code()}")
            print(f"Detalles del error: {e.details()}")
        except Exception as e:
            print(e)
        """ 
        
        """    
        try:
            request = key_value_store_service_pb2.StatRequest()
            result = stub.Stat(request)
            print(f"El servidor ha iniciado el: {result.time_started}")
            print(f"El total de peticiones son: {result.total_requests}")
            print(f"El total de Set: {result.total_set_requests}")
            print(f"El total de Get: {result.total_get_requests}")
            print(f"El total de GetPrefix: {result.total_get_prefix_requests}")
        except grpc.RpcError as e:
            print(f"Error al intentar ejecutar Stat")
            print(f"StatusCode: {e.code()}")
            print(f"Detalles del error: {e.details()}")
        except Exception as e:
            print(e)
        """     
    print("Conexion Cerrada")
    
if __name__ == "__main__":
    main()