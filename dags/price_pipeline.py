import pendulum
from airflow.sdk import dag, task
from airflow.timetables.trigger import CronTriggerTimetable

from src.extract import fetch_dummyjson, normalize_dummyjson
from src.load import get_conn, get_source, get_external_ids, load_staging, upsert_final
from src.raw_store import save_raw, load_raw

SOURCE_ID = 1
RAW_DIR = "/opt/airflow/data/raw"


def get_slot(context):
    """Créneau logique en UTC, arrondi à l'heure. Identique à chaque relance."""
    ts = context.get("data_interval_start") or context.get("logical_date")
    if ts is None:                       # déclenchement manuel sans date logique
        ts = context["dag_run"].run_after
    ts = ts.in_timezone("UTC").replace(minute=0, second=0, microsecond=0)
    return ts.replace(tzinfo=None)       # datetime "naïf" UTC pour MySQL


@dag(
    dag_id="price_pipeline",
    schedule=CronTriggerTimetable("0 8,20 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=2)},
    tags=["price-tracker"],
)
def price_pipeline():

    @task
    def extract_raw(**context):
        slot = get_slot(context)
        conn = get_conn()
        try:
            name, base_url = get_source(conn, SOURCE_ID)
            external_ids = get_external_ids(conn, SOURCE_ID)
        finally:
            conn.close()
        raw, _ = fetch_dummyjson(base_url, external_ids)
        return str(save_raw(name, slot, raw, base_dir=RAW_DIR))

    @task
    def stage(**context):
        slot = get_slot(context)
        conn = get_conn()
        try:
            name, _ = get_source(conn, SOURCE_ID)
            raw = load_raw(name, slot, base_dir=RAW_DIR)
            rows = [normalize_dummyjson(p) for p in raw]
            return load_staging(conn, rows, SOURCE_ID, slot)
        finally:
            conn.close()

    @task
    def upsert(**context):
        slot = get_slot(context)
        conn = get_conn()
        try:
            upsert_final(conn, SOURCE_ID, slot)
        finally:
            conn.close()

    extract_raw() >> stage() >> upsert()


price_pipeline()