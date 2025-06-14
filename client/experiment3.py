import grpc
import threading
import time
import random
import string
import matplotlib.pyplot as plt

from concurrent.futures import ThreadPoolExecutor
from key_value_store_service_pb2 import SetKeyValue, GetValue
from key_value_store_service_pb2_grpc import KeyValueStoreStub

SERVER_ADDRESS = "localhost:50051"
VALUE_SIZE = 1024  # Tamaño valor en bytes (1 KB)
NUM_KEYS = 1000  # Número de claves de prueba
DURATION = 10  # Duración del test en segundos
CLIENT_COUNTS = [1, 2, 4, 8, 16, 32]  # Diferentes números de clientes

# Generar claves y valores de prueba aleatorios
test_keys = [f"key_{i}" for i in range(NUM_KEYS)]
test_values = [''.join(random.choices(string.ascii_letters, k=VALUE_SIZE)) for _ in range(NUM_KEYS)]

# Hilo que realiza solo operaciones de lectura mientras no se detenga
def worker_read_only(stub, latencies, ops_counter, stop_event):
    local_count = 0
    while not stop_event.is_set():
        key = random.choice(test_keys)
        start = time.time()
        try:
            stub.Get(GetValue(key=key))
            latency_ms = (time.time() - start) * 1000  # Latencia en ms
            latencies.append(latency_ms)
            ops_counter.append(1)  # Contador de operaciones

            local_count += 1
            if local_count % 100 == 0:
                print(f"GET ops: {local_count} | Latencia: {latency_ms:.3f} ms")

        except Exception as e:
            print(f"[ERROR] GET clave '{key}': {e}")


# Hilo que realiza operaciones de lectura o escritura (50% cada una)
def worker_read_write(stub, latencies, ops_counter, stop_event):
    local_count = 0
    while not stop_event.is_set():
        key = random.choice(test_keys)
        if random.random() < 0.5:
            # Lectura
            start = time.time()
            try:
                stub.Get(GetValue(key=key))
                latency_ms = (time.time() - start) * 1000
                latencies.append(latency_ms)
                ops_counter.append(1)

                local_count += 1
                if local_count % 100 == 0:
                    print(f"GET ops: {local_count} | Latencia: {latency_ms:.3f} ms")

            except:
                pass
        else:
            # Escritura
            value = random.choice(test_values)
            start = time.time()
            try:
                stub.Set(SetKeyValue(key=key, value=value))
                latency_ms = (time.time() - start) * 1000
                latencies.append(latency_ms)
                ops_counter.append(1)

                local_count += 1
                if local_count % 100 == 0:
                    print(f"SET ops: {local_count} | Latencia: {latency_ms:.3f} ms")

            except:
                pass

def run_worker_thread(worker_func, latencies, ops_counter, stop_event):
    with grpc.insecure_channel(SERVER_ADDRESS) as channel:
        stub = KeyValueStoreStub(channel)
        worker_func(stub, latencies, ops_counter, stop_event)

#Ejecuta el test con el número de clientes y modo (solo lectura o mixto).
def run_test(num_clients, read_only=True):
    latencies = []
    ops_counter = []
    stop_event = threading.Event()

    # Selecciona función del worker según tipo de carga
    worker_func = worker_read_only if read_only else worker_read_write

    # Crear y lanzar hilos de clientes
    threads = []
    for _ in range(num_clients):
        thread = threading.Thread(target=run_worker_thread, args=(worker_func, latencies, ops_counter, stop_event))
        thread.start()
        threads.append(thread)

    time.sleep(DURATION)  # Durar la prueba
    stop_event.set()  # Señal para detener los hilos

    # Esperar que terminen los hilos
    for t in threads:
        t.join()

    # Calcular métricas
    total_ops = len(ops_counter)
    avg_latency = sum(latencies) / len(latencies) if latencies else 0
    throughput = total_ops / DURATION

    return avg_latency, throughput


def main():
    results_read = []
    results_mixed = []

    print("== Experimento Solo Lectura ==")
    for clients in CLIENT_COUNTS:
        print(f"{clients} cliente(s)...")
        latency, throughput = run_test(clients, read_only=True)
        print(f"Latencia promedio: {latency:.3f} ms | Rendimiento: {throughput:.2f} ops/s")
        results_read.append((latency, throughput))

    print("\n== Experimento 50% Lectura / 50% Escritura ==")
    for clients in CLIENT_COUNTS:
        print(f"{clients} cliente(s)...")
        latency, throughput = run_test(clients, read_only=False)
        print(f"Latencia promedio: {latency:.3f} ms | Rendimiento: {throughput:.2f} ops/s")
        results_mixed.append((latency, throughput))

    # Graficar resultados
    lat_read, thr_read = zip(*results_read)
    lat_mix, thr_mix = zip(*results_mixed)

    plt.figure(figsize=(10, 6))
    plt.plot(thr_read, lat_read, marker='o', label='Solo Lectura')
    plt.plot(thr_mix, lat_mix, marker='s', label='50% Lectura / 50% Escritura')
    plt.xlabel("Rendimiento (ops/s)")
    plt.ylabel("Latencia promedio (ms)")
    plt.title("Latencia vs Rendimiento")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("experiment3_results.png")
    plt.show()


if __name__ == "__main__":
    main()
