from metric_watch.constants import (
    CREATE_OR_REPLACE_VIEW_AS,
    FORMULA_TEMPLATE,
    TEMPLATE_GROUPBY_PART,
    TEMPLATE_GROUPBY_SLICE_PART,
    TEMPLATE_PART_COUNTRY,
    TEMPLATE_PART_PLATFORM,
)
from metric_watch.helpers import metrics
from metric_watch.metrics_config import Metric, Granularity, DateRange
import logging
import os

logging.basicConfig(level=logging.INFO)

# Define paths
CURRENT_DIR = os.path.dirname(__file__)
TEMPLATE_DIR = os.path.join(CURRENT_DIR, "../templates")
OUTPUT_DIR = os.path.join(CURRENT_DIR, "../artefacts")


def save_query(
    metric: Metric,
    granularity: Granularity,
    content: str,
    slice_name: str = "",
    slice_value: str = "",
    date_range: DateRange | None = None,
) -> None:
    """Save SQL view query to a file."""
    slice = {"country": TEMPLATE_PART_COUNTRY, "platform": TEMPLATE_PART_PLATFORM}.get(
        slice_name, ""
    )
    date_range = date_range if date_range else metric.date_range
    end_date_for_name = (
        "" if date_range.end == "now()" else "__" + date_range.end.replace("-", "_")
    )
    end_date = "now()" if date_range.end == "now()" else f"toDate('{date_range.end}')"
    template = CREATE_OR_REPLACE_VIEW_AS + content
    full_query = (
        template.replace("{name}", metric.name)
        .replace("__{slice}", f"__{slice_value.lower()}" if slice_value else "")
        .replace("{slice},", slice)
        .replace(
            "{where}", f"    AND {slice_name} = '{slice_value}'" if slice_name else ""
        )
        .replace(
            TEMPLATE_GROUPBY_PART,
            (
                TEMPLATE_GROUPBY_SLICE_PART.format(slice=slice_name)
                if slice_name
                else TEMPLATE_GROUPBY_PART
            ),
        )
        .replace("__{end_date}", end_date_for_name)
        .replace("{end_date}", end_date)
        .replace("{granularity}", granularity.name)
        .replace("{window_size}", str(granularity.window_size))
    )
    view_name = full_query.split(".")[1].split("\n")[0]

    with open(os.path.join(OUTPUT_DIR, f"{view_name}.sql"), "w") as f:
        f.write(full_query)

    logging.info(
        f"Metric query saved: {metric.name} by {granularity.name} for {slice_value}"
    )


def get_usual_template(name: str) -> str:
    """Get SQL view template for a metric."""
    template_files = [
        f
        for f in os.listdir(TEMPLATE_DIR)
        if os.path.isfile(os.path.join(TEMPLATE_DIR, f))
    ]
    file_path = next((f for f in template_files if f"template__{name}.sql" == f), None)
    if not file_path:
        raise FileNotFoundError(f"Template file not found for {name}")

    with open(os.path.join(TEMPLATE_DIR, file_path), "r") as file:
        return str(file.read())


def get_formula_template(formula: str) -> str:
    """Get SQL view template for a formula."""
    numerator, denominator = formula.split(" / ")

    return FORMULA_TEMPLATE.replace("{name}", numerator, 1).replace(
        "{name}", denominator
    )


def get_template(target: Metric) -> str:
    """Get SQL view template"""
    if target.formula:
        return get_formula_template(target.formula)

    return get_usual_template(target.name)


def generate_queries(
    metric: Metric,
    granularities: list[Granularity] | None = None,
    date_range: DateRange | None = None,
) -> None:
    """Save SQL views for metric granularities."""
    template = get_template(metric)

    for g in granularities if granularities else metric.granularities:
        save_query(
            metric=metric,
            granularity=g,
            content=template,
            date_range=date_range,
        )

        for slice in g.slices:
            for value in slice.values:
                save_query(
                    metric=metric,
                    granularity=g,
                    slice_name=slice.name,
                    slice_value=value,
                    content=template,
                    date_range=date_range,
                )


def get_metric_if_exists(target: str) -> Metric:
    """Get a metric by name."""
    metric = next((m for m in metrics if m.name == target), None)
    if not metric:
        raise ValueError(f"Metric {target} not found")

    return metric


def run_generation(metrics: list[Metric]) -> None:
    """Runs SQL views queries generation for metrics."""
    for metric in metrics:
        if metric.formula:
            # generate queries for prerequisites
            for target in metric.formula.split(" / "):
                generate_queries(
                    metric=get_metric_if_exists(target),
                    # pass metric properties according to config
                    # we don't want to pass target's properties here
                    granularities=metric.granularities,
                    date_range=metric.date_range,
                )
        generate_queries(metric)

    logging.info("Generated views have been saved here:")
    logging.info(OUTPUT_DIR)
