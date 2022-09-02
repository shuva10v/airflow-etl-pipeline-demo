## Airflow ETL pipeline demo

Demo pipeline for Airflow 2.x: fetches BTC/USD converstion rate from exchangerate.host
and stores it in Postgres DB.

## Running

[docker-compose.yaml](./docker-compose.yaml) based on
[recommended docker-compose from Airflow community](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html).
Also, postgres container added.

Steps to run:

1. Start containers:
```
docker-compose up -d
```
2. Create DB connections:
```
docker-compose run airflow-worker airflow connections add 'dwh_db' \
  --conn-uri postgresql+psycopg2://airflow:airflow@postgres-dwh/dwh
```

3. Run DAG ``btc_usd_demo_etl`` from UI (default login/pass: ``airflow/airflow``) 
or via CLI:
```
docker-compose run airflow-worker airflow dags backfill btc_usd_demo_etl --start-date $(date "+%Y-%m-%d")
```
4. Check DAG results in postgres:
```
 docker-compose exec postgres-dwh psql -U airflow dwh -c "select * from rates"
```