import time
import random
import statistics
from client.lbclient import KVClient, generate_value
import matplotlib.pyplot as plt

import subprocess
import sys
import os

from concurrent.futures import ThreadPoolExecutor # Necesario para gestionar los hilos

KEY_COUNT = 1000
ITERATIONS = 100

VALUE_SIZES = [512, 4096, 524288, 1048576, 4194304]  # bytes

server_script = os.path.abspath("./server/lbserver.py")


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

# Solo lectura
def benchmark_read_only(client, keys):
    latencies = []
    for i in range(ITERATIONS):
        key = random.choice(keys)
        start = time.time()
        client.get(key)
        end = time.time()
        latencies.append(end - start)
        if i % 10 == 0:  # Mostrar progreso cada 10 iteraciones
            print(f"[READ] {i + 1}/{ITERATIONS} | Key = {key} | Latencia = {(end - start):.6f}s")
        if (i + 1) % 100 == 0:
            print(f"[READ] {i + 1}/{ITERATIONS} | Key = {key} | Latencia = {(end - start):.6f}s")
    return latencies

# Lectura y escritura 50/50
def benchmark_mixed(client, keys, value):
    latencies = []
    for i in range(ITERATIONS):
        key = random.choice(keys)
        if random.random() < 0.5:
            start = time.time()
            client.get(key)
            end = time.time()
            operation_type = "GET"
        else:
            start = time.time()
            client.set(key, value)
            end = time.time()
            operation_type = "SET"
        latencies.append(end - start)
        if i % 10 == 0:
            print(f"[MIXED] {i + 1}/{ITERATIONS} | OPERACION = {operation_type} | Key = {key} | Latencia = {(end - start):.6f}s")
        if (i + 1) % 100 == 0:
            print(f"[MIXED] {i + 1}/{ITERATIONS} | OPERACION = {operation_type} | Key = {key} | Latencia = {(end - start):.6f}s")
    return latencies

def run_experiment():
    # Inicia el servidor
    proc = start_server()
        
        # Espera a que el servidor esté listo
    wait_time = wait_for_server_ready()
    if wait_time is None:
            print("No se pudo iniciar el servidor. Abortando.")
            return
    try: 
        client = KVClient()
        keys = [f"key{i}" for i in range(KEY_COUNT)]

        print("Inicializando claves...")
        for i, key in enumerate(keys):
            client.set(key, "init")
            if (i + 1) % 100 == 0:
                print(f" - {i + 1} claves inicializadas")

        read_only_results = []
        mixed_results = []

        for size in VALUE_SIZES:
            print(f"\n== Tamaño de valor: {size // 1024} KB ==")
            value = generate_value(size)

            print("Sobrescribiendo claves con el nuevo valor...")
            for i, key in enumerate(keys):
                client.set(key, value)
            print(" - Claves actualizadas.")

            print(">> Cargando solo lectura...")
            read_latencies = benchmark_read_only(client, keys)
            read_avg = statistics.mean(read_latencies)
            read_only_results.append(read_avg)
            print(f"[READ ONLY] Latencia promedio: {read_avg:.6f}s")

            print(">> Carga 50% lectura / 50% escritura...")
            mixed_latencies = benchmark_mixed(client, keys, value)
            mixed_avg = statistics.mean(mixed_latencies)
            mixed_results.append(mixed_avg)
            print(f"[MIXED] Latencia promedio: {mixed_avg:.6f}s")

        client.close()

        # Graficar resultados
        sizes_kb = [s // 1024 for s in VALUE_SIZES]
        plt.figure(figsize=(10, 6))
        plt.plot(sizes_kb, read_only_results, marker='o', label="Solo lectura")
        plt.plot(sizes_kb, mixed_results, marker='s', label="Lectura/Escritura 50/50")

        plt.title("Latencia promedio vs Tamaño del valor")
        plt.xlabel("Tamaño del valor (KB)")
        plt.ylabel("Latencia promedio (segundos)")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig("experimento1_latencias.png")
        plt.show()
    except Exception as e:
        print(f"{e}")

    finally:
        # Detener el servidor al finalizar
        stop_server(proc)

if __name__ == "__main__":
    run_experiment()
