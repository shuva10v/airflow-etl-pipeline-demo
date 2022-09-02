from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import json
import requests

@dag(
    schedule_interval=timedelta(hours=3),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
)
def btc_usd_demo_etl():
    """
    Simple ETL pipeline demo: fetches BTC/USD converstion rate from exchangerate.host
    and stores it in Postgres DB
    """

    create_rates_table = PostgresOperator(
        task_id="create_rates_table",
        postgres_conn_id="dwh_db",
        sql="""
        CREATE TABLE IF NOT EXISTS rates (
            "item" VARCHAR(100),
            "time" TIMESTAMP,
            "value" FLOAT,
            PRIMARY KEY ("time", "item")
        );""",
    )

    @task
    def fetch_data_task():
        res = requests.get("https://api.exchangerate.host/latest?base=BTC")
        if res.status_code == 200:
            return res.text
        else:
            raise Exception(f"Unable to fetch date: {res.status_code}: {res.text}")

    @task
    def parse_btc_usd(raw: str):
        obj = json.loads(raw)
        rate = obj.get('rates', {}).get('USD', None)
        if rate is None:
            raise Exception(f"Unable to get rates from response: {raw}")
        return rate

    @task
    def insert_value_to_dwh(value):
        query = """
        INSERT INTO rates(item, time, value) VALUES ('USD/BTC', now(), %s)
        """ % value
        postgres_hook = PostgresHook(postgres_conn_id="dwh_db")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()

    create_rates_table >> insert_value_to_dwh(parse_btc_usd(fetch_data_task()))

btc_usd_demo_etl_dag = btc_usd_demo_etl()

    