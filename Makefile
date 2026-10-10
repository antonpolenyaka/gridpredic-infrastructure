# Atajos para el trabajo diario. En Windows funcionan desde Git Bash o WSL;
# el equivalente en PowerShell para la prueba completa es _runlogs/run_silver_test.ps1.

.PHONY: help up down restart ps logs config test test-silver test-gold test-ml lint bronze silver gold train clean

help:
	@echo "make up        levanta el stack (docker compose up -d --build --wait)"
	@echo "make down      para el stack sin borrar volúmenes"
	@echo "make ps        estado de los servicios"
	@echo "make logs      logs de todos los servicios (S=nombre para uno solo)"
	@echo "make config    valida compose.yaml con el .env actual"
	@echo "make test      tests de Silver, Gold y modelo en un Spark local, sin Docker"
	@echo "make test-silver / make test-gold / make test-ml   solo una parte"
	@echo "make lint      ruff check"
	@echo "make bronze    lanza el DAG batch de Bronze en Airflow"
	@echo "make silver    lanza el DAG de Silver en Airflow"
	@echo "make gold      lanza el DAG de Gold en Airflow"
	@echo "make train     entrena y evalua los modelos en valid (ARGS=--evaluar-test para test)"
	@echo "make clean     borra cachés locales de Python y de pytest"

up:
	docker compose up -d --build --wait

down:
	docker compose down

restart: down up

ps:
	docker compose ps

logs:
	docker compose logs -f $(S)

config:
	docker compose config --quiet && echo "compose.yaml OK"

test:
	python -m pytest tests -q

test-silver:
	python -m pytest tests/02_silver -q

test-gold:
	python -m pytest tests/03_gold -q

test-ml:
	python -m pytest tests/04_ml -q

lint:
	ruff check .

bronze:
	docker compose exec airflow airflow dags trigger ingest_sqlserver_batch_bronze

silver:
	docker compose exec airflow airflow dags trigger dag_silver

gold:
	docker compose exec airflow airflow dags trigger dag_gold

train:
	docker compose exec spark-master bash /app/jobs/04_ml/train.sh $(ARGS)

clean:
	find . -name "__pycache__" -type d -prune -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache
