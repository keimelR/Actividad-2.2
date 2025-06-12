import key_value_store_service_pb2
import key_value_store_service_pb2_grpc

from concurrent import futures
import os
import threading
import grpc
import datetime

import struct

class KeyValueServer(key_value_store_service_pb2_grpc.KeyValueStoreServicer):
    def __init__(self):
        # Diccionario para guardar los par clave-valor
        self.data = {}
        
        # Archivo que guarda los pares clave-valor
        self.file = open("./server/database.log", "ab")
        
        # Hilo para gestionar la concurrencia
        self.lock = threading.Lock()
        
        # Metricas del servidor
        self.time_started = datetime.datetime.now().isoformat()
        self.total_requests = 0
        self.total_set_requests = 0
        self.total_get_requests = 0
        self.total_get_prefix_requests = 0
        
        # Almacena los clave-valor en el diccionario
        self.recover_data()
        
    def recover_data(self):
        """ Lee la base de datos 'database.log' y almacena las claves y valores en el diccionario """
        try:
            # Abre el archivo 'database.log' en formato de lectura binaria
            with open("./server/database.log", "rb") as file:
                while True:
                    # Obtenemos el header (compuesto por 4 bytes de la clave y 4 bytes del valor)
                    header = file.read(8)
                    
                    # En caso de que el header no tenga los 8 bytes, se entiende que hubo un fallo al escribir la informacion (fallo de luz, de proceso, etc)
                    if len(header) < 8:
                        break

                    # Descomprimimos la cadena de bytes del header en el clave y valor (4 bits para key_len y 4 bits para value_len)
                    key_len, value_len = struct.unpack(">II", header)
                    key = file.read(key_len)
                    value = file.read(value_len)

                    if len(key) < key_len or len(value) < value_len:
                        break
                    
                    # Almacenamos en el diccionario el par clave-valor decodificado en texto. 
                    self.data[key.decode()] = value.decode()
        except FileNotFoundError:
            pass
        
    def write_entry(self, key, value):
        """ Serializa en datos binarios las peticiones SET del cliente """
        
        # Codificamos la clave y valor en utf-8
        key_formated = key.encode("utf-8")
        value_formated = value.encode("utf-8")
        
        # Comprimimos en una cadena de byte las longitudes de la clave y valor
        header = struct.pack(">II", len(key_formated), len(value_formated))
        
        # Escribimos en el archivo la cabecera, clave y valor y sincronizamos el archivo para asegurarnos que es escriba correctamente
        self.file.write(header + key_formated + value_formated)
        self.file.flush()
        os.fsync(self.file.fileno())
        
    def Get(self, request, context):
        """ Devuelve el valor de la clave dada """
        with self.lock:  
            # Obtenemos el valor en el diccionario a partir de la clave
            value = self.data.get(request.key)
            
            # Si no existe un valor, el cliente recibe un Status.Not_Found y un mensaje del error
            if value is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                response = key_value_store_service_pb2.GetValueResponse(status = False, value = f"Clave no encontrada")
                return response
                
            # Objeto que sera enviado al cliente con el valor de la clave dada
            response = key_value_store_service_pb2.GetValueResponse(status = True, value = value)

            # Incrementamos el numero de peticiones totales y peticiones get
            self.total_requests += 1
            self.total_get_requests += 1
            
            print(f"Se ha recibido una peticion de Get")
            return response
    
    def Set(self, request, context):
        """ Establece el valor de la clave dada """
        with self.lock:
            # Obtenemos la clave y valor enviado por el cliente
            key = request.key
            value = request.value
               
            # Serializamos los datos
            self.write_entry(key, value)
            
            # Guardamos en el diccionario
            self.data[key] = value
            
            # Incrementamos el numero de peticiones totales y peticiones set
            self.total_set_requests += 1
            self.total_requests += 1
            
            print("Se ha recibido una peticion Set")
            
            # Enviamos al usuario la respuesta con los datos realizados
            response = key_value_store_service_pb2.SetKeyValueResponse(status = True, message = f"Set: {request.key} = {request.value}")
            return response
    
    def GetPrefixKey(self, request, context):
        """ Devuelve una lista de valores cuyas claves empiezan por prefixKey """
        with self.lock:
            # Iniciamos una lista de claves y valores (para valores coincidentes)
            keys = []
            values = []
            
            # Recorremos el diccionario en busca de claves que empiecen por prefixKey, en caso de encontrar, se le añade a las listas de claves y valores
            for key, value in self.data.items():
                if key.startswith(request.prefixKey):
                    keys.append(key)
                    values.append(value)
            
            # Objeto que sera enviado al cliente con la respuesta deseada (lista de valores que empiezan por la prefixKey)
            response = key_value_store_service_pb2.GetPrefixResponse(keys = keys, values = values)
            
            # Incrementamos el numero de peticiones totales y peticiones get_prefix
            self.total_get_prefix_requests += 1
            self.total_requests += 1
            
            print("Se ha recibido una peticion getPrefix")
            return response
    
    def Stat(self, request, context):
        """ Recupera las estadísticas del servidor """
        with self.lock:
            # Objeto con todas las estadisticas del servidor
            response = key_value_store_service_pb2.StatResponse(
                time_started = datetime.datetime.now().isoformat(),
                total_requests = self.total_requests,
                total_set_requests = self.total_set_requests,
                total_get_requests = self.total_get_requests,
                total_get_prefix_requests = self.total_get_prefix_requests
            )
            print("Se ha recibido una peticion Stat")
            
            # Se envia el objeto al cliente
            return response
    
    
def main():
    # Iniciamos el servidor gRPC
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Añadimos el servicio KeyValueStore al servidor
    key_value_store_service_pb2_grpc.add_KeyValueStoreServicer_to_server(KeyValueServer(), server)
    
    # Iniciamos el servidor en el puerto 50051
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server started on port 50051")
    
    try:
        # Mantenemos el servidor en ejecución hasta que se interrumpa manualmente
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("Server shutting down...")
        server.stop(0)
        
if __name__ == "__main__":
    main()