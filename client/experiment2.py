import time
import subprocess
import os
import random
from lbclient import KVClient, generate_value
import sys
import matplotlib.pyplot as plt

NUM_KEYS = 10000
VALUE_SIZE = 4096
HOT_READS = 1000

server_script = os.path.abspath("./server/lbserver.py")

def populate_store(client):
    print(f"Ingresando {NUM_KEYS} claves de {VALUE_SIZE} bytes...")
    for i in range(NUM_KEYS):
        key = f"key_{i}"
        value = generate_value(VALUE_SIZE)
        client.set(key, value)
        if (i + 1) % 1000 == 0:
            print(f" - {i + 1}/{NUM_KEYS} claves insertadas")
    print("Población del almacén completada.")

def measure_latency(client, description):
    print(f"\n=== Mediciones de {description} ===")
    print(f"Midendo latencia con {HOT_READS} lecturas aleatorias...")
    total_time = 0
    for i in range(HOT_READS):
        key = f"key_{random.randint(0, NUM_KEYS - 1)}"
        start = time.time()
        client.get(key)
        latency = time.time() - start
        total_time += latency
        if (i + 1) % 100 == 0:
            print(f"[{description}] {i + 1}/{HOT_READS} | Key = {key} | Latencia = {latency * 1000:.3f} ms")
    avg_latency = (total_time / HOT_READS) * 1000
    print(f"Latencia promedio: {avg_latency:.3f} ms")
    return avg_latency

def plot_latency_results(hot_latency, cold_latency, restart_duration):
    labels = ['Latencia en caliente (ms)', 'Latencia en frío (ms)', 'Reinicio del servidor (s)']
    values = [hot_latency, cold_latency, restart_duration]
    colors = ['#4caf50', '#2196f3', '#f44336']

    fig, ax = plt.subplots()
    bars = ax.bar(labels, values, color=colors)

    for bar in bars:
        height = bar.get_height()
        ax.annotate(f'{height:.2f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom')

    ax.set_ylabel('Tiempo (ms / s)')
    ax.set_title('Comparación de latencia y tiempo de reinicio')
    plt.xticks(rotation=15)
    plt.tight_layout()
    plt.grid(axis='y', linestyle='--', alpha=0.6)

    plt.savefig("experimento2_resultados.png")
    plt.show()

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

def run_experiment():
    proc = start_server()
    time.sleep(2)

    if wait_for_server_ready() is None:
        stop_server(proc)
        return

    client = KVClient()
    populate_store(client)

    hot_latency = measure_latency(client, "lectura en caliente")
    client.close()

    stop_server(proc)

    print("\nReiniciando servidor para medir latencias en frío...")
    start_restart = time.time()
    proc = start_server()
    restart_time = wait_for_server_ready()
    if restart_time is None:
        stop_server(proc)
        return
    restart_duration = time.time() - start_restart
    print(f"\nEl servidor tardó {restart_duration:.3f} segundos en reiniciarse y responder.")

    client = KVClient()
    cold_latency = measure_latency(client, "lectura en frío")
    client.close()

    stop_server(proc)

    print("\n===== COMPARACIÓN FINAL =====")
    print(f"Lectura en caliente: {hot_latency:.3f} ms")
    print(f"Lectura en frío:    {cold_latency:.3f} ms")
    print(f"Tiempo de reinicio: {restart_duration:.3f} segundos")

    plot_latency_results(hot_latency, cold_latency, restart_duration)

if __name__ == "__main__":
    run_experiment()
