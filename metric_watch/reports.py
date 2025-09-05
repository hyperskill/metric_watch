from dotenv import load_dotenv  # type: ignore
from infi.clickhouse_orm import F  # type: ignore
from infi.clickhouse_orm.query import QuerySet  # type: ignore
import json
from os import getenv as env
from metric_watch.constants import LOGS
from metric_watch.charts import (
    generate_chart_link,
    get_access_token,
    get_chart_id_by_name,
)
from metric_watch.database import db
from metric_watch.helpers import metrics, name_to_metric
from metric_watch.metrics_config import Metric
from metric_watch.tables import MetricWatchLog, Subscriptions
import logging

logging.basicConfig(level=logging.INFO)
load_dotenv()

SS_URL: str = env("SS_URL", "")
SS_USER_NAME: str = env("SS_USER_NAME", "")
SS_PASSWORD: str = env("SS_PASSWORD", "")

required_env_vars = ["SS_URL", "SS_USER_NAME", "SS_PASSWORD"]
missing_env_vars = [var for var in required_env_vars if not env(var)]
if missing_env_vars:
    raise EnvironmentError(
        f"Missing required environment variables: {', '.join(missing_env_vars)}"
    )


def get_logs(
    metric: str | None = None,
    start_dt: F = F.now() - F.toIntervalDay(1),
    end_dt: F = F.now(),
    is_alert: int = 0,
    **kwargs,
) -> QuerySet:
    """
    Get alerts' records for the specified period and metrics.
    """
    qs = MetricWatchLog.objects_in(db).filter(
        is_alert=is_alert,
        dt__gte=start_dt,
        dt__lte=end_dt,
    )
    if metric:
        qs = qs.filter(metric_name__in=[metric])
    if kwargs.get("is_regular"):
        qs = qs.filter(is_regular=1)
    if kwargs.get("user"):
        qs = qs.filter(user=kwargs["user"])

    return qs


def format_record(alert: QuerySet, indent: int = 0) -> str:
    """
    Format a single alert record with indentation.
    """
    if alert.count() == 0:
        logging.warning("No alerts to format")
        return ""

    alert = alert[0]
    if alert.metric_last_dt is None:
        logging.warning("No metric_last_dt in the alert")
        return ""

    metric_last_dt = alert.metric_last_dt.strftime("%Y-%m-%d %H:%M:%S (UTC +0)")
    chart = (
        ":chart_with_upwards_trend:"
        if alert.deviation_percent > 0
        else ":chart_with_downwards_trend:"
    )
    delimeter = "–" * 25
    indentation = "  " * indent

    chart_link = "Error generating chart link"
    access_token = get_access_token(SS_URL, SS_USER_NAME, SS_PASSWORD)
    chart_id = get_chart_id_by_name(
        base_url=SS_URL,
        token=access_token,
        chart_name=alert.model_name,
    )
    if chart_id:
        chart_link = generate_chart_link(chart_id)

    return (
        f"{indentation}| {metric_last_dt} |\n"
        f"{indentation}`{alert.model_name}` value: `{alert.metric_value}`\n"
        f"{indentation}Difference with distribution AVG: `{alert.deviation_percent:.1f} %` {chart}\n"
        f"{indentation}Distribution average: `{alert.dist_avg:.2f}`\n"
        f"{indentation}<{chart_link}|Chart Link>\n"
        f"{indentation}{delimeter}\n"
    )


def weave_chapter(metric: Metric, metric_alerts: QuerySet) -> str:
    """
    Weave a "chapter" for a metric. A chapter is a string of concatenated
    alerts for the metric: its granularities and slices.
    """
    chapter = ""
    for granularity in metric.granularities:
        granularity_alerts = metric_alerts.filter(granularity=granularity.name)
        chapter += format_record(
            granularity_alerts.filter(slice=None).order_by("-dt").limit_by(1, "dt"),
            indent=1,
        )
        if chapter:
            if granularity.slices:
                for slice in granularity.slices:
                    for slice_value in slice.values:
                        chapter += format_record(
                            granularity_alerts.filter(slice=slice_value)
                            .order_by("-dt")
                            .limit_by(1, "dt"),
                            indent=2,
                        )

    return chapter


def weave_stories(alerted_metrics: set[str], alerts: QuerySet) -> dict[str, str]:
    """
    Weave a case "story" from chapters. A chapter is a string of concatenated
    alerts for the metric: its granularities and slices. A story is a chapter
    or several ones if a metric has metrics-dependencies.
    """
    global metrics, name_to_metric
    stories: dict[str, str] = {}

    for metric in metrics:
        if metric.name not in alerted_metrics:
            continue
        filtered_alerts = alerts.filter(metric_name=metric.name)
        stories[metric.name] = weave_chapter(metric, filtered_alerts)

    # assemble stories with dependencies
    duplicates = []
    for story in stories:
        if story not in name_to_metric:
            raise ValueError(f"Metric {metric} not found in the metrics list")

        metric = name_to_metric[story]
        if metric.depends_on:
            for dependency in metric.depends_on:
                if dependency in stories:
                    stories[metric.name] += stories[dependency]
                    duplicates.append(dependency)

    stories = {k: v for k, v in stories.items() if len(v) > 0 and k not in duplicates}

    return stories


def make_report(metrics: list[str], stories: dict[str, str]) -> str:
    """
    Create a report for the user.
    """
    report = ""
    for metric, story in stories.items():
        report += f">*{metric}*:\n{story}\n"

    return report


def get_user_to_metrics(metrics: set[str]) -> dict[str, list[str]]:
    """
    Provide a dictionary with users and their metrics.
    """
    user_metrics: dict[str, list[str]] = {}
    for record in Subscriptions.objects_in(db).filter(metric__in=metrics):
        if record.user in user_metrics:
            user_metrics[record.user].append(record.metric)
        else:
            user_metrics[record.user] = [record.metric]

    return user_metrics


def collect_reports(**kwargs) -> list[dict[str, str]]:
    """
    Creates list of ready-to-send reports if alerts are existed.
    """
    reports: list[dict[str, str]] = []

    if kwargs.get("run_type") == LOGS:
        logs = get_logs(**kwargs)
        return [{"user": "_", "report": format_record(logs)}]

    alerts = get_logs(is_alert=1, **kwargs)
    if alerts.count() == 0:
        logging.info("No alerts for the period")
        return reports

    alerted_metrics = set([a.metric_name for a in alerts])
    user_to_metrics = get_user_to_metrics(metrics=alerted_metrics)
    stories = weave_stories(alerted_metrics, alerts)

    for user, subscriptions in user_to_metrics.items():
        reports.append(
            {
                "user": user,
                "report": make_report(metrics=subscriptions, stories=stories),
            }
        )

    return reports


def main(**kwargs) -> None:
    output_file = kwargs.get("file")
    reports = collect_reports(**kwargs)

    if output_file:
        with open(output_file, "w") as f:
            json.dump(reports, f)
        if reports:
            logging.info(f"Reports written to {output_file}")

    for report in reports:
        if report["user"] in ("UNCMCAQ12", "_"):
            report_text = report["report"].replace("|Chart Link", "")
            logging.info(f"Reports for {report['user']}:\n{report_text}")

    if not reports:
        logging.info("No reports to send")


if __name__ == "__main__":
    main()
