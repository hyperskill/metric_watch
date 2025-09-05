from datetime import timedelta, datetime
from airflow.decorators import dag, task
from helpers import metric_watch_docker


DEFAULT_ARGS = {
    "owner": "Vladimir Klimov",
    "email": ["PUT_YOUR_EMAIL_HERE"],
    "retries": 1,
    "retry_delay": timedelta(minutes=6),
    "email_on_failure": True,
    "start_date": datetime(2024, 8, 7, 7, 7, 7),
}


@dag(
    tags=["MetricWatch"],
    default_args=DEFAULT_ARGS,
    schedule_interval=timedelta(hours=1),
)
def metric_watch_search():
    """
    Search anomalies for MetricWatch subscribers.
    """

    @task
    def search(**kwargs):
        current_datetime_str = datetime.now().strftime("%Y-%m-%d--%H-%M-%S")
        operator = metric_watch_docker(
            task_id="search",
            command="python main.py --action=search",
            container_name=f"metric_watch_search__{current_datetime_str}",
        )
        operator.execute(dict())

    search()


dag = metric_watch_search()
