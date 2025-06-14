.PHONY: run install clean
run:
	@echo "Descargando las dependencias..."
	pip install -r requirements.txt

	@echo "Insertando pares clave-valor a database.log..."
	python -m client.poblar_servidor

	@echo "Ejecutando el experimento 1..."
#	python -m client.experiment1

	@echo "Ejecutando el experimento 2..."
#	python -m client.experiment2

	@echo "Ejecutando el experimento 3..."
#	python -m client.experiment3

	@echo "Ejecutando el Script para los Stats del Servidor..."
#	python -m client.script_stat

install:
	@echo "Descargando las dependencias..."
	pip install -r requirements.txt

clean:
	rm -rf dist
	rm -rf build
	rm -rf __pycache__
	rm -rf *.spec