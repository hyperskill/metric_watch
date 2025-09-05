from airflow.decorators import dag, task
from datetime import datetime
from helpers import (
    metric_watch_docker,
    send_slack_notification,
    create_dir_if_not_exists,
)
import json


DEFAULT_ARGS = {
    "owner": "Vladimir Klimov",
    "email": ["PUT_YOUR_EMAIL_HERE"],
    "email_on_failure": True,
    "schedule_interval": "@once",
}

MWC_PATH = "artifacts/metric_watch_custom"
FILE_NAME = "reports_{user}.json"
INPUT_FILE = f"{MWC_PATH}/{FILE_NAME}"
OUTPUT_FILE = "/" + INPUT_FILE
NO_MESSAGE = "MetricWatch: there are no anomalies :female-detective:"

create_dir_if_not_exists(MWC_PATH)


@dag(tags=["MetricWatch"], default_args=DEFAULT_ARGS)
def metric_watch_custom(**kwargs):
    """Run MetricWatch from Slack."""

    @task
    def run(**kwargs):
        """Run MetricWatch tool."""
        current_datetime_str = datetime.now().strftime("%Y-%m-%d--%H-%M-%S")
        dag_run = kwargs.get("dag_run")
        slack_user = dag_run.conf.get("slack_user", "no_user")
        args = dag_run.conf.get("args", [])
        args.append(f"--user={slack_user}")
        output_file = OUTPUT_FILE.replace("{user}", slack_user)

        operator = metric_watch_docker(
            task_id="run",
            command=f"python main.py --file={output_file} " + " ".join(args),
            container_name=f"metric_watch_custom__{slack_user}__{current_datetime_str}",
        )
        operator.execute(dict())

    @task
    def send_reports(**kwargs):
        """Send reports to Slack."""
        dag_run = kwargs.get("dag_run")
        slack_user = dag_run.conf.get("slack_user")
        input_file = INPUT_FILE.replace("{user}", slack_user)

        with open(input_file, "r") as file:
            reports = json.load(file)

        if not reports:
            send_slack_notification(slack_user, NO_MESSAGE).execute(dict())
            return

        for report in reports:
            send_slack_notification(slack_user, report["report"]).execute(dict())

    run() >> send_reports()


dag = metric_watch_custom()
