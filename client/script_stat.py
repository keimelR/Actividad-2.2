import time
import subprocess
import os
import random
import sys
import grpc

from lbclient import KVClient, generate_value
from key_value_store_service_pb2 import SetKeyValue, GetValue, GetPrefix # Importar GetPrefix

from concurrent.futures import ThreadPoolExecutor # Necesario para gestionar los hilos

SERVER_ADDRESS = "localhost:50051"
VALUE_SIZE = 512  # Tamaño valor en bytes (512 bytes)
NUM_KEYS = 1000   # Número de claves a pre-poblar en el almacén
NUM_CLIENTS = 10   # Número de clientes concurrentes
OPERATIONS_PER_CLIENT = 1000 # Operaciones objetivo por cada cliente (10K)

# Ruta absoluta al script del servidor
server_script = os.path.abspath("./server/lbserver.py")

# Generar claves y valores de prueba aleatorios
test_keys = [f"key_{i}" for i in range(NUM_KEYS)]

# Los valores iniciales para poblar el almacén (se pueden generar una vez)
test_value = [generate_value(VALUE_SIZE) for _ in range(NUM_KEYS)]

def start_server():
    print("\nIniciando el servidor...")
    # sys.executable asegura que se use el mismo intérprete Python
    return subprocess.Popen([sys.executable, server_script])

def stop_server(proc):
    print("\nDeteniendo el servidor...")
    proc.terminate()
    proc.wait()
    print("Servidor detenido.")
    
# Espera hasta que el servidor esté listo para aceptar peticiones usando stat cada 0.5 segundos.
def wait_for_server_ready(timeout=120):
    print("Esperando que el servidor esté listo...")
    start = time.time()
    client = KVClient()
    while time.time() - start < timeout:
        try:
            client.stat()  # Comprueba si el servidor está disponible
            client.close()
            print("Servidor listo.")
            return time.time() - start
        except:
            time.sleep(0.5)
    print("El servidor no respondió a tiempo.")
    return None

# Hilo para clientes concurrentes
def worker_client_operations(client_id, operations_to_perform, is_mixed_mode):
    # Cada hilo debe tener su propia instancia de KVClient
    local_client = KVClient(SERVER_ADDRESS) 
    
    for i in range(operations_to_perform):
        key = random.choice(test_keys)
        
        # Decide si es GET, SET o GET_PREFIX
        op_choice = random.random()
        
        try:
            if is_mixed_mode and op_choice < 0.5: # 50% SET
                # Generar nuevo valor para cada SET
                value = generate_value(VALUE_SIZE)
                local_client.set(key, value)
                operation_type = "SET"
            elif op_choice < 0.75: 
                # Buscar un valor segun el prefijo de una clave
                prefix_key = "key_" + str(random.randint(0, NUM_KEYS // 100)) 
                local_client.get_prefix(prefix_key)
                operation_type = "GET_PREFIX"
            else:
                # Buscar un valor segun una clave
                local_client.get(key)
                operation_type = "GET"
                
            if i % 100 == 0:
                print(f"| {i + 1} / {NUM_KEYS} | OPERACION = {operation_type} | Key = {key} | CLIENTE = {client_id}")
        except grpc.RpcError:
            # Silenciar errores para no inundar la consola, pero podrías loguearlos
            pass 
        except Exception:
            # Silenciar errores generales también
            pass

    local_client.close() # Cerrar la conexión del cliente del hilo

# Ejecuta el test con el número de clientes y modo solicitado
def run_test_load(num_clients, total_operations_per_client, read_only=True):
    print(f"\n--- Ejecutando carga con {num_clients} clientes ({total_operations_per_client} ops/cliente) ---")
    
    # ThreadPoolExecutor gestiona los hilos
    with ThreadPoolExecutor(max_workers=num_clients) as executor:
        for i in range(num_clients):
            # submit es no bloqueante, ejecuta el worker en un hilo
            executor.submit(worker_client_operations, i + 1, total_operations_per_client, not read_only)
            
    print("Todos los clientes han completado sus operaciones.")


def main():
    proc = None
    start_of_experiment_wall_time = time.time()
    
    try:
        # Inicia el servidor
        proc = start_server()
        
        # Espera a que el servidor esté listo
        wait_time = wait_for_server_ready()
        if wait_time is None:
            print("No se pudo iniciar el servidor. Abortando.")
            return

        print(f"\nServidor iniciado y listo en {wait_time:.2f} segundos.")  

        # Ejecuta los clientes concurrentes
        run_test_load(NUM_CLIENTS, OPERATIONS_PER_CLIENT, read_only=False)

        # Obtener estadísticas finales del servidor
        final_stats_client = KVClient(SERVER_ADDRESS)
        final_server_stats = final_stats_client.stat()
        final_stats_client.close()

        # Paso 6: Mostrar estadísticas
        print("\n--- Estadísticas Finales del Experimento ---")
        print(f"Tiempo de inicio del servidor: {final_server_stats.time_started}")
        print(f"Total SETs completados: {final_server_stats.total_set_requests}")
        print(f"Total GETs completados: {final_server_stats.total_get_requests}")
        print(f"Total GET_PREFIXes completados: {final_server_stats.total_get_prefix_requests}")
        print(f"Total de peticiones procesadas por el servidor: {final_server_stats.total_requests}")
        print(f"Tiempo total de ejecución del script: {(time.time() - start_of_experiment_wall_time):.2f} segundos")

    except Exception as e:
        print(f"{e}")

    finally:
        # Paso 7: Detener el servidor al finalizar
        stop_server(proc)

if __name__ == "__main__":
    main()