from infi.clickhouse_orm import Database  # type: ignore
from metric_watch.constants import METRIC_WATCH_DB
from dotenv import load_dotenv  # type: ignore
from os import environ as env

load_dotenv()


db = Database(
    db_name=METRIC_WATCH_DB,
    db_url=str(env.get("CLICKHOUSE_HOST_URL")),
    username=env.get("CH_USER"),
    password=env.get("CH_PASSWORD"),
)
