from airflow.decorators import dag, task
from datetime import datetime
from helpers import (
    metric_watch_docker,
    send_slack_notification,
    create_dir_if_not_exists,
    format_product_report,
)
import json


DEFAULT_ARGS = {
    "owner": "Vladimir Klimov",
    "email": ["PUT_YOUR_EMAIL_HERE"],
    "email_on_failure": True,
}

MWC_PATH = "artifacts/metric_watch_product"
FILE_NAME = "reports__{user}__{metric}.json"
INPUT_FILE = f"{MWC_PATH}/{FILE_NAME}"
OUTPUT_FILE = "/" + INPUT_FILE
NO_MESSAGE = "MetricWatch: there are no anomalies :female-detective:"
METRICS = [
    "new_visitors",
    "new_registered_users",
    "1h_activation_rate",
    "new_subscribers",
    "10d_subscription_rate",
    "2nd_week_retention_rate",
]
CHANNEL = "#product"

create_dir_if_not_exists(MWC_PATH)


@dag(
    tags=["MetricWatch"],
    start_date=datetime(2024, 8, 12),
    # run dag every Monday at 02:00
    schedule_interval="0 2 * * 1",
    catchup=False,
    default_args=DEFAULT_ARGS,
)
def metric_watch_product(**kwargs):
    """Run MetricWatch from Slack."""

    @task
    def run_mw(**kwargs):
        """Run MetricWatch tool."""
        current_datetime_str = datetime.now().strftime("%Y-%m-%d--%H-%M-%S")
        default_args = ["--granularity=week", "--window=24", "--run_type=logs"]

        for metric in METRICS:
            args = default_args + [f"--metric={metric}"]
            args.append(f"--user={CHANNEL}")
            output_file = OUTPUT_FILE.replace("{user}", CHANNEL).replace(
                "{metric}", metric
            )
            operator = metric_watch_docker(
                task_id="run_mw",
                command=f"python main.py --file={output_file} " + " ".join(args),
                container_name=f"metric_watch_product__{CHANNEL}__{current_datetime_str}".replace(
                    "#", ""
                ),
            )
            operator.execute(dict())

    @task
    def send_reports(**kwargs):
        """Send reports to Slack."""
        result = ""

        for metric in METRICS:
            input_file = INPUT_FILE.replace("{user}", CHANNEL).replace(
                "{metric}", metric
            )
            with open(input_file, "r") as file:
                result += json.load(file)[0].get("report") + "\n"
        print(f"DEBUG:\n{result}")
        send_slack_notification(CHANNEL, format_product_report(result)).execute(dict())

    run_mw() >> send_reports()


dag = metric_watch_product()
