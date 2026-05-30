# streaming_pipeline
Data pipeline for streaming events from Kafka

## Run

Prerequisites:
- Docker & Docker Compose
- Python 3.8+ (for the local producer)

1. Start infrastructure (Kafka, Zookeeper, Spark UI):

```powershell
docker-compose up -d
```

2. Start the producer (runs on the host; uses mapped localhost:9092):

```powershell
pip install -r producers/requirements.txt
python producers\order_producer.py
```

3. Submit the Spark streaming job (runs inside the `spark` container):

```powershell
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/jobs/kafka_to_bronze.py
```

4. Inspect the bronze data (optional):

```powershell
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/test/df_test.py
```

Notes:
- The host producer uses `bootstrap_servers='localhost:9092'` (host-mapped port). If you run the producer inside a container, set the broker to `kafka:29092`.
- Spark writes Parquet files to the mounted path `./data/bronze/orders` and checkpoints to `./data/checkpoints/orders`.

Want me to start the services and run the job now?
