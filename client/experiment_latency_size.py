import time
import random
import statistics
from lbclient import KVClient, generate_value

# Configuración del experimento
KEY_COUNT = 1000
VALUE_SIZES = [512, 4096, 524288, 1048576, 4194304]  # En bytes
ITERATIONS = 100  # Iteraciones por experimento

def benchmark_read_only(client, keys):
    latencies = []
    for _ in range(ITERATIONS):
        key = random.choice(keys)
        start = time.time()
        client.get(key)
        end = time.time()
        latencies.append(end - start)
    return latencies

def benchmark_mixed(client, keys, value):
    latencies = []
    for _ in range(ITERATIONS):
        key = random.choice(keys)
        if random.random() < 0.5:
            start = time.time()
            client.get(key)
            end = time.time()
        else:
            start = time.time()
            client.set(key, value)
            end = time.time()
        latencies.append(end - start)
    return latencies

def run_experiment():
    client = KVClient()
    keys = [f"key{i}" for i in range(KEY_COUNT)]

    print("Inicializando claves con valor pequeño...")
    for key in keys:
        client.set(key, "init")

    for size in VALUE_SIZES:
        print(f"\n== Valor de tamaño: {size} bytes ==")
        value = generate_value(size)

        print("Sobrescribiendo claves con valores grandes...")
        for key in keys:
            client.set(key, value)

        print(">> Ejecutando carga de solo lectura...")
        read_latencies = benchmark_read_only(client, keys)
        print(f"[READ ONLY] Latencia promedio: {statistics.mean(read_latencies):.6f}s")

        print(">> Ejecutando carga 50/50 lectura/escritura...")
        mixed_latencies = benchmark_mixed(client, keys, value)
        print(f"[MIXED] Latencia promedio: {statistics.mean(mixed_latencies):.6f}s")

    client.close()

if __name__ == "__main__":
    run_experiment()
