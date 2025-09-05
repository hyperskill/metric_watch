from collections import deque
from clickhouse_driver import Client, errors  # type: ignore
import datetime
from metric_watch.constants import PATH_TO_METRIC_HIERARCHY, SELECT_ALL_QUERY
from metric_watch.database import db
from metric_watch.metrics_config import MetricsConfig, Metric, Granularity
from metric_watch.tables import MetricWatchLog
import logging
import os
from pandas import DataFrame
from typing import List, Any

logging.basicConfig(level=logging.INFO)

# Set ClickHouse connection
ch_client = Client(
    host=os.getenv("CH_HOST"),
    user=os.getenv("CH_USER"),
    password=os.getenv("CH_PASSWORD"),
)


def get_metrics() -> List[Metric]:
    """
    Get metrics from the configuration file.
    """
    if not os.path.exists(PATH_TO_METRIC_HIERARCHY):
        raise FileNotFoundError(
            f"Metrics configuration file not found at {PATH_TO_METRIC_HIERARCHY}"
        )
    with open(PATH_TO_METRIC_HIERARCHY, "r") as file:
        return MetricsConfig.from_yaml(file).metrics


def config_is_correct(metrics: list[Metric]) -> bool:
    """
    Check the metric configuration file.
    1. Check if `depends_on` metrics are in the configuration.
    2. ... TODO: Add more checks.
    """

    def check_depends_on(metric: Metric) -> bool:
        """
        Check if the `depends_on` metrics are in the configuration.
        """
        for dependency in metric.depends_on:
            if dependency not in name_to_metric:
                raise ValueError(f"Metric {dependency} not found in the configuration")

        return True

    for metric in metrics:
        check_depends_on(metric)

    return True


metrics = get_metrics()
name_to_metric = {metric.name: metric for metric in metrics}
if not config_is_correct(metrics):
    raise ValueError("Metrics configuration is not correct")


def get_data(formatted_query: str) -> DataFrame:
    """
    Execute the formatted query and return the result as a DataFrame.
    """
    try:
        query_data, columns = ch_client.execute(formatted_query, with_column_types=True)
    except errors.ServerException as e:
        logging.error(f"ClickHouse ServerException: {str(e).splitlines()[1]}")
        return DataFrame()

    return DataFrame(query_data, columns=[c[0] for c in columns]).sort_values(
        by="dt",
        ascending=False,
    )


def log_metrics(func):
    def wrapper(*args, **kwargs):
        start_time = datetime.datetime.now(tz=datetime.timezone.utc)
        if "metric" not in kwargs:
            raise ValueError("Keyword parameter 'metric' is not provided!")
        if "granularity" not in kwargs:
            raise ValueError("Keyword parameter 'granularity' is not provided!")

        metric = kwargs.get("metric")
        granularity = kwargs.get("granularity")
        slice = (
            kwargs.get("slice").replace(" ", "_").lower()
            if kwargs.get("slice")
            else None
        )
        end_date = metric.date_range.end.replace("-", "_")
        template = (
            SELECT_ALL_QUERY.replace("__{slice}", f"__{slice}" if slice else "")
            .replace("__{end_date}", f"__{end_date}" if end_date != "now()" else "")
            .format(
                name=metric.name,
                granularity=granularity.name,
                window_size=granularity.window_size,
            )
        )
        model_name = template.split(".")[1]
        result = func(*args, query=template, **kwargs)
        log = MetricWatchLog(
            dt=start_time,
            is_alert=(
                result.get("is_anomaly") if result.get("is_anomaly") in [0, 1] else 2
            ),
            metric_name=metric.name,
            model_name=model_name,
            metric_last_dt=result.get("last_dt"),
            granularity=granularity.name,
            window_size=granularity.window_size,
            sigma_n=granularity.sigma_n,
            slice=kwargs.get("slice"),
            dist_avg=result.get("distribution_avg"),
            std_dev=result.get("distribution_std"),
            metric_value=result.get("last_value"),
            deviation=result.get("deviation"),
            deviation_percent=result.get("deviation_percent"),
            is_regular=kwargs.get("is_regular"),
            run_type=(
                "default" if kwargs.get("run_type") is None else kwargs.get("run_type")
            ),
            user="default" if kwargs.get("user") is None else kwargs.get("user"),
        )

        db.insert([log])

        return result

    return wrapper


@log_metrics
def is_anomaly(
    metric: Metric,
    granularity: Granularity,
    slice: str | None = None,
    query: str = SELECT_ALL_QUERY,
    **kwargs,
) -> dict[str, Any]:
    """Check if the current metric value is an anomaly."""

    def make_calculation(metric_df: DataFrame, **kwargs) -> dict[str, Any]:
        """
        Make calculations for the metric.
        """
        logging.info("  make_calculation ->\n" + metric_df.head(1).to_string())

        is_regular = kwargs.get("is_regular")
        rows = metric_df.head(1).itertuples() if is_regular else metric_df.itertuples()

        for row in rows:
            dt = row.dt
            value: Any = row.metric
            subset = metric_df[metric_df["dt"] != dt]
            distribution_avg = subset.metric.mean()
            distribution_std = subset.metric.std()
            deviation = value - distribution_avg
            deviation_percent = deviation / distribution_avg * 100
            is_anomaly = abs(deviation) > granularity.sigma_n * distribution_std

            result = {
                "is_anomaly": is_anomaly,
                "last_dt": dt,
                "last_value": value,
                "distribution_avg": distribution_avg,
                "distribution_std": distribution_std,
                "deviation": deviation,
                "deviation_percent": deviation_percent,
            }
            # return the only first founded anomaly TODO: improve?
            if is_anomaly:
                logging.info(
                    f"    Anomaly detected for {metric.name}, {granularity.name}, {slice}"
                )
                logging.info(f"      {result}")
                return result

        return result

    metric_df = get_data(query)
    if metric_df.empty:
        logging.warn(
            f"An empty DataFrame for {metric.name}, {granularity.name}, {slice}"
        )
        return {"is_anomaly": False}

    return make_calculation(metric_df, **kwargs)


def is_alerted(metric: Metric, **kwargs) -> bool:
    """Check metric for anomalies."""
    logging.info(f"Visiting {metric.name}")
    result = False

    for granularity in metric.granularities:
        logging.info(f"  Checking {granularity}")
        kwargs.update({"granularity": granularity})
        if is_anomaly(metric=metric, **kwargs)["is_anomaly"]:
            result = True
            if granularity.slices:
                for slice in granularity.slices:
                    for value in slice.values:
                        logging.info(f"    Slice: {value}")
                        kwargs.update({"slice": value})
                        is_anomaly(metric=metric, **kwargs)

    return result


def search_anomalies(targets: list[Metric], **kwargs) -> None:
    """Search for anomalies in the metrics."""
    global name_to_metric
    queue = deque(targets)
    visited = set()

    while queue:
        current_metric = queue.popleft()
        if current_metric in visited:
            continue
        visited.add(current_metric)
        kwargs.update({"metric": current_metric})

        if is_alerted(**kwargs):
            if current_metric.depends_on:
                for dependency in current_metric.depends_on:
                    queue.append(name_to_metric[dependency])
