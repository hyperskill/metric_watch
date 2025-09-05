"""
Anomaly Detection CLI

Usage:
  main.py (-h | --help)
  main.py [--action=<action>] [--start=<start>] [--end=<end>] [--metric=<metric>]
          [--granularity=<granularity>] [--window=<window>] [--sigma=<sigma>]
          [--slice=<slice>] [--file=<file>] [--run_type=<run_type>] [--user=<user>]

Options:
  -h --help                         Show this screen
  --action=<action>                 Action to perform (search, report)
  --start=<start>                   Start date for the anomaly search
  --end=<end>                       End date for the anomaly search
  --metric=<metric>                 Metric name from metrics_hierarchy.yml
  --granularity=<granularity>       Granularity name (hour, day, week, month)
  --window=<window>                 Window size – days ago from a specific date
  --sigma=<sigma>                   Sigma value – number of std errors to trigger an alert
  --slice=<slice>                   Slice value – slice values from metrics_hierarchy.yml
  --file=<file>                     File to read reports from
  --run_type=<run_type>             Type of run ("logs" if you want just the lates metrics values)
  --user=<user>                     User

Examples:
    python main.py --start=2024-01-01 --end=2024-04-01  `: check all metrics for the date range`
    python main.py --start=2024-01-01 --end=2024-04-01 --metric=new_registered_users
    python main.py --metric=new_registered_users --granularity=hour --window=168 --sigma=3 --slice=web
    python main.py --metric=new_registered_users --granularity=hour --window=168 --sigma=3
    python main.py --metric=new_registered_users --granularity=hour
    python main.py --metric=new_registered_users  `: check all metric granularities`
    python main.py --slice=Android  `: filter all metrics from the configuration that have the slice Android`
    python main.py  `: run the anomaly search for all metrics from the configuration`
"""

import logging
from docopt import docopt
from datetime import datetime
from os import makedirs
from metric_watch.constants import FILTERS, MUTAGENS
from metric_watch.create_charts import run as create_charts
from metric_watch.helpers import search_anomalies, metrics, name_to_metric
from metric_watch.generate_queries import run_generation
from metric_watch.metrics_config import Metric, Granularity, Slice, DateRange
from metric_watch.reports import main as provide_reports
from metric_watch.views import create as create_views
from shutil import rmtree

logging.basicConfig(level=logging.INFO)


def log_arguments(args: dict) -> None:
    """Log parsed arguments."""
    logging.info("\nParsed arguments:")
    for key, value in args.items():
        logging.info(f"  {key}: {value}")
    logging.info("")


def parse_arguments() -> dict:
    """Parse command-line arguments."""

    def validate_date_range(start_date: str, end_date: str):
        """Validate the date range."""
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            if start >= end:
                raise ValueError("Start date must be before end date")
        except ValueError as e:
            raise ValueError(f"Invalid date range: {e}")

    arguments = docopt(__doc__)
    if arguments.get("--start") and arguments.get("--window"):
        raise ValueError("Cannot use both 'start' and 'window' arguments")
    if arguments.get("--start") and arguments.get("--end"):
        validate_date_range(arguments["--start"], arguments["--end"])

    result = {k.replace("--", ""): v for k, v in arguments.items()}
    result["is_regular"] = (
        False
        if set(FILTERS + MUTAGENS).intersection(
            {k for k, v in result.items() if v is not None}
        )
        else True
    )
    if result.get("run_type") == "logs":
        result["is_regular"] = True

    log_arguments(result)
    return result


def get_metric_by_name(metric_name: str) -> Metric:
    """Retrieve a metric by its name from the configuration."""
    metric = name_to_metric.get(metric_name)
    if not metric:
        raise ValueError(f"Metric {metric_name} not found in the configuration")
    return metric


def filter_config(**kwargs) -> None:
    """Filter the configuration based on the arguments."""
    global metrics

    if "metric" in kwargs:
        metrics = [get_metric_by_name(kwargs["metric"])]
        return  # Skip other filters if a metric is found

    if "slice" in kwargs:
        metrics = [
            metric
            for metric in metrics
            if any(
                kwargs.get("slice") in slice.values
                for granularity in metric.granularities
                for slice in granularity.slices
            )
        ]


def create_granularities(
    for_metric: Metric,
    start: str,
    end: str,
    granularity: str,
    window: int,
    sigma: int,
    slice: str,
    **kwargs,
) -> list:
    """Create a list of granularities for a metric."""
    if not granularity:
        granularity = "day"
    if not sigma:
        sigma = 3
    slices = (
        [Slice(name=slice, values=[slice])]
        if slice
        else [g.slices for g in for_metric.granularities if g.name == granularity][0]
    )
    if start is not None and end is not None:
        try:
            delta = datetime.strptime(end, "%Y-%m-%d") - datetime.strptime(
                start, "%Y-%m-%d"
            )
        except ValueError as e:
            raise ValueError(f"Invalid date range: {e}")
        else:
            if granularity == "hour":
                window = delta.days * 24
            elif granularity == "day":
                window = delta.days
            elif granularity == "week":
                window = delta.days // 7
            elif granularity == "month":
                window = delta.days // 30

    return [
        Granularity(
            name=granularity,
            window_size=window,
            sigma_n=sigma,
            slices=slices,
        )
    ]


def mutate_config(**kwargs) -> None:
    """Mutate the configuration based on the arguments."""
    global metrics
    mutated_metrics = []

    for metric in metrics:
        granularities = (
            create_granularities(for_metric=metric, **kwargs)
            if any([kwargs.get("granularity"), kwargs.get("start")])
            else None
        )
        date_range = DateRange(kwargs["start"], kwargs["end"])
        mutated_metrics.append(
            Metric(
                name=metric.name,
                description=metric.description,
                formula=metric.formula,
                depends_on=metric.depends_on,
                granularities=granularities if granularities else metric.granularities,
                date_range=(
                    date_range
                    if any([kwargs["start"], kwargs["end"]])
                    else metric.date_range
                ),
            )
        )

    metrics = mutated_metrics


def prepare_config(**kwargs) -> None:
    """Prepare tool run configurations."""
    if any(kwargs.get(m) for m in FILTERS):
        filter_config(**kwargs)

    if any(kwargs.get(m) for m in MUTAGENS):
        mutate_config(**kwargs)


def clean_artefacts() -> None:
    """Remove previous run artefacts."""
    rmtree("artefacts", ignore_errors=True)
    makedirs("artefacts")


def create_entities() -> None:
    """Create database and visualization entities."""
    global metrics

    clean_artefacts()
    run_generation(metrics)
    create_views()
    create_charts()


def main() -> None:
    """Run the tool based on the command-line arguments."""
    kwargs = parse_arguments()
    prepare_config(**kwargs)
    if "action" in kwargs:
        if kwargs["action"] == "search":
            search_anomalies(metrics, **kwargs)
            return
        if kwargs["action"] == "report":
            provide_reports(**kwargs)
            return

    create_entities()
    search_anomalies(metrics, **kwargs)
    provide_reports(**kwargs)


if __name__ == "__main__":
    main()
