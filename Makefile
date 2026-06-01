.PHONY: up down produce stream train

up:
	docker-compose up -d

down:
	docker-compose down

produce:
	python data/generator/kafka_producer.py

stream:
	spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
		processing/streaming/streaming_anomaly_job.py

train:
	python ml_pipeline/train_isolation_forest.py

serve:
	uvicorn serving.api.fastapi_app:app --reload --port 8000
