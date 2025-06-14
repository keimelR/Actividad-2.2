import time
from client.lbclient import KVClient, generate_value  # Cliente gRPC y generador de valores

import subprocess
import sys
import os

NUM_KEYS = 10000           # Número de claves a insertar
VALUE_SIZE = 1024          # Tamaño de cada valor (1 KB)

server_script = os.path.abspath("./server/lbserver.py")

def start_server():
    print("\nIniciando el servidor...")
    return subprocess.Popen([sys.executable, server_script])

def stop_server(proc):
    print("\nDeteniendo el servidor...")
    proc.terminate()
    proc.wait()
    print("Servidor detenido.")

def wait_for_server_ready(timeout=120):
    print("Esperando que el servidor esté listo...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            client = KVClient()
            client.stat()
            client.close()
            print("Servidor listo.")
            return time.time() - start
        except:
            time.sleep(0.5)
    print("El servidor no respondió a tiempo.")
    return None

def populate_store(client):
    print(f"Ingresando {NUM_KEYS} claves de {VALUE_SIZE} bytes...")
    for i in range(NUM_KEYS):
        key = f"key_{i}"
        value = generate_value(VALUE_SIZE)
        client.set(key, value)

        # Reducir el ruido en consola
        if (i + 1) % 100 == 0 or i == NUM_KEYS - 1:
            print(f"| {i + 1} / {NUM_KEYS} | OPERACION = SET | Key = {key} |")
    print(" Población completada.")

def main():
    try:
        # Inicia el servidor
        proc = start_server()
            
        # Espera a que el servidor esté listo
        wait_time = wait_for_server_ready()
        if wait_time is None:
                print("No se pudo iniciar el servidor. Abortando.")
                return
        
        client = KVClient()  # Asume que el servidor ya está corriendo y disponible
        populate_store(client)
        client.close()
        
        # Detener el servidor al finalizar
        stop_server(proc)  
    except Exception as e:
        print(f" Error al poblar el almacén: {e}")

if __name__ == "__main__":
    main()
