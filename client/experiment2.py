import time
import subprocess
import os
import random
from lbclient import KVClient, generate_value  # Cliente y generador de valores
import sys
import matplotlib.pyplot as plt

NUM_KEYS = 10000           # Número de claves a insertar en el almacén
VALUE_SIZE = 4096          # Tamaño de cada valor (4 KB)
HOT_READS = 1000           # Número de lecturas aleatorias para medir latencia

# Ruta absoluta al script del servidor que será lanzado como subproceso
server_script = os.path.abspath("./server/lbserver.py")

#inserta NUM_KEYS pares clave-valor
def populate_store(client):
    print(f"Ingresando {NUM_KEYS} claves de {VALUE_SIZE} bytes...")
    for i in range(NUM_KEYS):
        key = f"key_{i}"
        value = generate_value(VALUE_SIZE)
        client.set(key, value)
        print(f"| {i + 1} / {NUM_KEYS} | OPERACION = SET | Key = {key} |")

    print("Población del almacén completada.")


#Mide la latencia promedio de HOT_READS operaciones GET aleatorias.
def measure_latency(client, description):
    print(f"\n=== Mediciones de {description} ===")
    print(f"Midendo latencia con {HOT_READS} lecturas aleatorias...")
    total_time = 0
    for i in range(HOT_READS):
        key = f"key_{random.randint(0, NUM_KEYS - 1)}"
        start = time.time()
        client.get(key)
        total_time += (time.time() - start)
        print(f"| {i + 1} / {NUM_KEYS} | OPERACION = GET | Key = {key} | Latencia = {(time.time() - start):.6f}")

    avg_latency = (total_time / HOT_READS) * 1000  # Convierte a milisegundos
    print(f"Latencia promedio: {avg_latency:.3f} ms")
    return avg_latency

def plot_latency_results(hot_latency, cold_latency, restart_duration):
    """ Genera el grafico de barra de la latencia """
    
    labels = ['Latencia en caliente (ms)', 'Latencia en frío (ms)', 'Reinicio del servidor (s)']
    values = [hot_latency, cold_latency, restart_duration]
    colors = ['#4caf50', '#2196f3', '#f44336']

    fig, ax = plt.subplots()
    bars = ax.bar(labels, values, color=colors)

    # Añade etiquetas con los valores numéricos encima de cada barra
    for bar in bars:
        height = bar.get_height()
        ax.annotate(f'{height:.2f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),  # desplazamiento vertical
                    textcoords="offset points",
                    ha='center', va='bottom')

    ax.set_ylabel('Tiempo (ms / s)')
    ax.set_title('Comparación de latencia y tiempo de reinicio')
    plt.xticks(rotation=15)
    plt.tight_layout()
    plt.grid(axis='y', linestyle='--', alpha=0.6)

    plt.show()

def start_server():
    print("\nIniciando el servidor...")
    return subprocess.Popen([sys.executable, server_script])


def stop_server(proc):
    print("\nDeteniendo el servidor...")
    proc.terminate()
    proc.wait()
    print("Servidor detenido.")


#Espera hasta que el servidor esté listo para aceptar peticiones usando stat cada 0.5 segundos.
def wait_for_server_ready(timeout=30):
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


def run_experiment():

    # Paso 1: Inicia el servidor
    proc = start_server()
    time.sleep(2)  # Pausa breve antes de verificar disponibilidad

    if wait_for_server_ready() is None:
        stop_server(proc)
        return

    # Paso 2: Conecta el cliente e inserta datos
    client = KVClient()
    populate_store(client)

    # Paso 3: Medición de latencias en caliente (servidor activo)
    hot_latency = measure_latency(client, "lectura en caliente")
    client.close()

    # Paso 4: Detiene y reinicia el servidor
    stop_server(proc)

    print("\nReiniciando servidor para medir latencias en frío...")
    start_restart = time.time()
    proc = start_server()
    restart_time = wait_for_server_ready()
    if restart_time is None:
        stop_server(proc)
        return
    restart_duration = time.time() - start_restart
    print(f"\n El servidor tardó {restart_duration:.3f} segundos en reiniciarse y responder.")

    # Paso 5: Medición de latencias en frío (después del reinicio)
    client = KVClient()
    cold_latency = measure_latency(client, "lectura en frío")
    client.close()

    # Paso 6: Detiene el servidor al finalizar
    stop_server(proc)

    # Paso 7: Imprime comparación de resultados
    print("\n===== COMPARACIÓN FINAL =====")
    print(f"Lectura en caliente: {hot_latency:.3f} ms")
    print(f"Lectura en frío:    {cold_latency:.3f} ms")
    print(f"Tiempo de reinicio: {restart_duration:.3f} segundos")
    
    plot_latency_results(hot_latency, cold_latency, restart_duration)

if __name__ == "__main__":
    run_experiment()
