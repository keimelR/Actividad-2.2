import time
from lbclient import KVClient, generate_value  # Cliente gRPC y generador de valores

NUM_KEYS = 10000           # Número de claves a insertar
VALUE_SIZE = 4096          # Tamaño de cada valor (4 KB)

def populate_store(client):
    print(f"Ingresando {NUM_KEYS} claves de {VALUE_SIZE} bytes...")
    for i in range(NUM_KEYS):
        key = f"key_{i}"
        value = generate_value(VALUE_SIZE)
        client.set(key, value)
        print(f"| {i + 1} / {NUM_KEYS} | OPERACION = SET | Key = {key} |")
    print(" Población completada.")

def main():
    try:
        client = KVClient()  # Asume que el servidor ya está corriendo y disponible
        populate_store(client)
        client.close()
    except Exception as e:
        print(f" Error al poblar el almacén: {e}")

if __name__ == "__main__":
    main()
