from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable
from docker.types import Mount
from airflow.operators.bash import BashOperator
import os
import re

def metric_watch_docker(command: str, container_name: str, **kwargs) -> DockerOperator:
    return DockerOperator(
        docker_conn_id=Variable.get("DOCKER_CONN_ID"),
        api_version="auto",
        container_name=container_name,
        cpus=1,
        image=Variable.get("METRIC_WATCH_IMAGE_NAME"),
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source=Variable.get("ARTIFACTS_PATH"),
                target="/artifacts",
                type="bind",
            )
        ],
        auto_remove=True,
        environment={
            "CH_HOST": Variable.get("CLICKHOUSE_HOST"),
            "CLICKHOUSE_HOST_URL": Variable.get("CLICKHOUSE_HOST_URL"),
            "CLICKHOUSE_HOST_IP": Variable.get("CLICKHOUSE_HOST_IP"),
            "CH_USER": Variable.get("CLICKHOUSE_AIRFLOW_USER"),
            "CH_PASSWORD": Variable.get("CLICKHOUSE_AIRFLOW_PASSWORD"),
            "SS_URL": Variable.get("SUPERSET_URL"),
            "SS_USER_NAME": Variable.get("SUPERSET_USER_NAME"),
            "SS_PASSWORD": Variable.get("SUPERSET_PASSWORD"),
            "SS_DATABASE_ID": Variable.get("SUPERSET_DATABASE_ID"),
        },
        docker_url=Variable.get("DOCKER_URL"),
        network_mode="bridge",
        command=command,
        **kwargs,
    )


def send_slack_notification(user_id: str, message: str) -> BashOperator:
    """
    Send a direct message to a Slack user.
    """
    slack_token = Variable.get("SLACK_BOT_TOKEN")
    return BashOperator(
        task_id=f"send_slack_notification_{user_id}".replace("#", ""),
        bash_command=(
            f"curl -X POST -H 'Authorization: Bearer {slack_token}' "
            f"-H 'Content-type: application/json' "
            f'--data \'{{"channel":"{user_id}", "text":"{message}"}}\' '
            "https://slack.com/api/chat.postMessage"
        ),
    )


def create_dir_if_not_exists(path: str) -> None:
    """
    Create a directory if it does not exist.
    """
    if not os.path.exists(path):
        os.makedirs(path)


def format_product_report(message: str) -> str:
    """
    Format a message for a product report.
    """
    URL = "https://superset.PUT_YOUR_DOMAIN_HERE.org/superset/explore/"
    metrics_n = message.count("Chart Link")

    week = re.findall(r"\| (\d{4}-\d{2}-\d{2}) ", message)
    links = re.findall(rf"<({URL}\S+)\|", message)
    metric = re.findall(r"(\w+?)__", message)
    value = [float(v) for v in re.findall(r"value: `(\d+\.\d+)", message)]
    diff_with_avg = [
        d + " %"
        for d in re.findall(
            r"Difference with distribution AVG: `([+-]?\d+\.\d+)", message
        )
    ]
    avg = [float(a) for a in re.findall(r"Distribution average: `(\d+\.\d+)", message)]
    table = f"```\n|{'Week':^16}|{'Metric':^28}|    AVG    |{'Value':^10}| Diff with AVG | Chart Link |\n"
    separator = (
        "|"
        + "–" * 16
        + "|"
        + "–" * 28
        + "|"
        + "–" * 11
        + "|"
        + "–" * 10
        + "|"
        + "–" * 15
        + "|"
        + "–" * 12
        + "|\n"
    )
    table += separator

    for i in range(metrics_n):
        table += f"| {week[i]:<14} | {metric[i].replace('_', ' ').title():<26} | {avg[i]:>9.2f} | {value[i]:>8.2f} | {diff_with_avg[i]:>13} | <{links[i]}|See Chart!> |\n"

    return table + "```"
