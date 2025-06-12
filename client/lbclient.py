import key_value_store_service_pb2
import key_value_store_service_pb2_grpc
from concurrent import futures
import grpc
    
    
def main():
    # Conectamos al servidor gRPC que se encuentra en localhost:50051
    with grpc.insecure_channel('localhost:50051') as channel:
        print("Conectando al servidor gRPC")
        
        # Creamos un stub para el servicio KeyValueStore
        stub = key_value_store_service_pb2_grpc.KeyValueStoreStub(channel)
        
        # Bloque de Codigo para ejecutar el metodo GetPrefixKey (devuelve una lista de valores cuyas claves empiezan por prefixKey)
        """
        try: 
            # Generamos la peticion al servidor, segun el prefixKey
            request = key_value_store_service_pb2.GetPrefix(prefixKey = "2")
            
            # Guardamos la respuesta del servidor
            result = stub.GetPrefixKey(request)
            
            # Imprimimos en pantalla la respuesta del servidor
            print(f"Claves: {result.keys}\nValores: {result.values}")
        except grpc.RpcError as e:
            print(f"Error al intentar ejecutar GetPrefix")
            print(f"StatusCode: {e.code()}")
            print(f"Detalles del error: {e.details()}")
        except Exception as e:
            print(e)
        """
            
        # Bloque de Codigo para ejecutar el metodo Set (establece el valor de la clave dada)
        try:
            # Generamos la peticion al servidor, segun la key y value
            request = key_value_store_service_pb2.SetKeyValue(key = "2", value = "Prueba Segunda")
            
            # Guardamos la respuesta del servidor
            result = stub.Set(request)
            
            # Imprimimos en pantalla la respuesta del servidor
            print(f"status = {(result.status)}\nmessage = {result.message}")
        except grpc.RpcError as e:
            print(f"Error al intentar ejecutar Set")
            print(f"StatusCode: {e.code()}")
            print(f"Detalles del error: {e.details()}")
        except Exception as e:
            print(e)

        """
        # Bloque de Codigo para ejecutar el metodo Get (Devuelve el valor de la clave dada) 
        try:
            # Generamos la peticion al servidor, segun la key
            request = key_value_store_service_pb2.GetValue(key = '1')
            
            # Guardamos la respuesta del servidor
            result = stub.Get(request)
            
            # Imprimimos en pantalla la respuesta del servidor
            print(f"GRPC received: {result}")

        except grpc.RpcError as e:
            print(f"Error al intentar ejecutar Get")
            print(f"StatusCode: {e.code()}")
            print(f"Detalles del error: {e.details()}")
        except Exception as e:
            print(e)
        """
        # Bloque de Codigo para ejecutar el metodo Stat (Recupera las estadísticas del servidor) 
        """    
        try:
            # Generamos la peticion al servidor
            request = key_value_store_service_pb2.StatRequest()
            
            # Guardamos la respuesta del servidor
            result = stub.Stat(request)
            
            # Imprimimos en pantalla la respuesta del servidor
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