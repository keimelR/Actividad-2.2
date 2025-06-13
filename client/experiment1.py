import time
import random
import statistics
from lbclient import KVClient, generate_value  # Cliente gRPC para comunicarse con el servidor y función para generar valores de cierto tamaño
import matplotlib.pyplot as plt


# Número total de claves a utilizar
KEY_COUNT = 1000

# Lista de tamaños de valores que se evaluarán (en bytes)
VALUE_SIZES = [
    512,        # 512 B
    4096,       # 4 KB
    524288,     # 512 KB
    1048576,    # 1 MB
    4194304     # 4 MB
]

# Número de operaciones a realizar en cada experimento (lecturas/escrituras)
ITERATIONS = 100

# FUNCIÓN: Solo Lectura
def benchmark_read_only(client, keys):
    latencies = []
    for _ in range(ITERATIONS):
        key = random.choice(keys)  # Selecciona una clave aleatoria
        start = time.time()        # Marca el inicio del tiempo
        client.get(key)            # Realiza una lectura
        end = time.time()          # Marca el fin del tiempo
        latencies.append(end - start)  # Calcula y guarda la latencia
    return latencies

# FUNCIÓN: 50% Lectura / 50% Escritura
def benchmark_mixed(client, keys, value):
    latencies = []
    for _ in range(ITERATIONS):
        key = random.choice(keys)
        if random.random() < 0.5:
            # Operación de lectura
            start = time.time()
            client.get(key)
            end = time.time()
        else:
            # Operación de escritura
            start = time.time()
            client.set(key, value)
            end = time.time()
        latencies.append(end - start)  # Registra la latencia de cada operación
    return latencies


# FUNCIÓN PRINCIPAL: Ejecuta el experimento
def run_experiment():
    client = KVClient()  # Inicializa el cliente gRPC
    keys = [f"key{i}" for i in range(KEY_COUNT)]  # Genera las claves: key0, key1, ..., key999

    print("Inicializando claves con valor pequeño...")
    for key in keys:
        client.set(key, "init")  # Pone un valor corto inicial en cada clave

    read_only_results = []
    mixed_results = []

    # Itera sobre cada tamaño de valor definido
    for size in VALUE_SIZES:
        print(f"\n== Valor de tamaño: {size} bytes ==")
        value = generate_value(size)  # Genera un valor aleatorio del tamaño especificado

        # Sobrescribe todas las claves con valores del tamaño actual
        print("Sobrescribiendo claves con valores grandes...")
        for key in keys:
            client.set(key, value)

        # Experimento 1: Solo lecturas
        print(">> Ejecutando carga de solo lectura...")
        read_latencies = benchmark_read_only(client, keys)
        
        read_latencies = benchmark_read_only(client, keys)
        read_avg = statistics.mean(read_latencies)
        
        print(f"[READ ONLY] Latencia promedio: {statistics.mean(read_latencies):.6f}s")
        read_only_results.append(read_avg)


        # Experimento 1.1: Lectura y escritura 50/50
        print(">> Ejecutando carga 50/50 lectura/escritura...")
        mixed_latencies = benchmark_mixed(client, keys, value)
        mixed_avg = statistics.mean(mixed_latencies)
        mixed_latencies = benchmark_mixed(client, keys, value)
        print(f"[MIXED] Latencia promedio: {statistics.mean(mixed_latencies):.6f}s")
        mixed_results.append(mixed_avg)

    client.close()  # Finaliza la conexión con el servidor
    
    # Generar gráfico
    plt.figure(figsize=(10, 6))
    sizes_kb = [s // 1024 for s in VALUE_SIZES]

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


if __name__ == "__main__":
    run_experiment()  
