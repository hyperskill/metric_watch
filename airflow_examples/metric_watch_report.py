from datetime import timedelta, datetime
from airflow.decorators import dag, task
import json
from helpers import metric_watch_docker, send_slack_notification


DEFAULT_ARGS = {
    "owner": "Vladimir Klimov",
    "email": ["PUT_YOUR_EMAIL_HERE"],
    "retries": 1,
    "retry_delay": timedelta(minutes=6),
    "email_on_failure": True,
    "start_date": datetime(2024, 8, 8, 6, 6, 6),
}

OUTPUT_FILE = "/artifacts/reports.json"
INPUT_FILE = "artifacts/reports.json"
START_MESSAGE = "DEBUG: MetricWatch Reports run..."
CHANNEL = 'XXXXXXX01'  # Use your Slack channel here


@dag(
    tags=["MetricWatch"], default_args=DEFAULT_ARGS, schedule_interval=timedelta(days=1)
)
def metric_watch_report():
    """
    Create and send reports to MetricWatch subscribers.
    """

    @task
    def report(**kwargs):
        current_datetime_str = datetime.now().strftime("%Y-%m-%d--%H-%M-%S")
        operator = metric_watch_docker(
            task_id="report",
            command=f"python main.py --action=report --file={OUTPUT_FILE}",
            container_name=f"metric_watch_report__{current_datetime_str}",
        )
        operator.execute(dict())

    @task
    def send_reports(**kwargs):
        """
        Send reports to Slack.
        """
        with open(INPUT_FILE, "r") as file:
            reports = json.load(file)

        if not reports:
            print("No reports to send")
            return

        dag_run = kwargs.get("dag_run")
        if dag_run.external_trigger:
            debug_reports = "\n".join([r["report"] for r in reports])
            send_slack_notification(
                CHANNEL, f"{START_MESSAGE}\n{debug_reports}"
            ).execute(dict())
            return

        for report in reports:
            send_slack_notification(report["user"], report["report"]).execute(dict())

    report() >> send_reports()


dag = metric_watch_report()
