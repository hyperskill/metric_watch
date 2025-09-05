import requests
from dotenv import load_dotenv  # type: ignore
from os import getenv as env
import logging
import json
from urllib import parse as urlparse
from typing import Any

load_dotenv()
logging.basicConfig(level=logging.INFO)

SS_URL: str = env("SS_URL", "")
SS_USER_ID: str = env("SS_USER_ID", "")
SS_USER_NAME: str = env("SS_USER_NAME", "")
SS_PASSWORD: str = env("SS_PASSWORD", "")
SS_DATABASE_ID: str = env("SS_DATABASE_ID", "")

required_env_vars = ["SS_URL", "SS_USER_NAME", "SS_PASSWORD", "SS_DATABASE_ID"]
missing_env_vars = [var for var in required_env_vars if not env(var)]
if missing_env_vars:
    raise EnvironmentError(
        f"Missing required environment variables: {', '.join(missing_env_vars)}"
    )


def get_access_token(base_url: str, username: str, password: str) -> str:
    """Fetches the access token for authentication."""
    url = f"{base_url}/api/v1/security/login"
    payload = {
        "username": username,
        "password": password,
        "provider": "db",
        "refresh": True,
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()

    return response.json().get("access_token")


def create_dataset(
    base_url: str, token: str, db_id: str, schema: str, table_name: str
) -> Any:
    """Adds a dataset to Superset."""
    payload = {"database": int(db_id), "schema": schema, "table_name": table_name}
    url = f"{base_url}/api/v1/dataset/"

    with requests.Session() as session:
        session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        )

        csrf_url = f"{base_url}/api/v1/security/csrf_token/"
        csrf_response = session.get(csrf_url)
        csrf_response.raise_for_status()
        csrf_token = csrf_response.json().get("result")

        session.headers.update({"Referer": csrf_url, "X-CSRFToken": csrf_token})

        response = session.post(url, json=payload)
        if response.status_code != 201:
            logging.info(
                f"Failed to create dataset: {response.status_code} {response.reason}"
            )
            if len(response.content) < 300:
                logging.info(f"Response content: {response.content.decode()}")
            else:
                logging.info("Response content is too long to display.")

        response.raise_for_status()

        return response.json()


def create_chart(
    base_url: str,
    token: str,
    dataset_id: int,
    name: str,
    prefix: str = "",
    granularity: str = "",
):
    """Creates a line chart in Superset."""
    url = f"{base_url}/api/v1/chart/"
    name = f"{prefix}{name}"
    time_grain_sqla = None
    payload = {
        "slice_name": name,
        "viz_type": "line",
        "params": json.dumps(
            {
                "time_range": "No filter",
                "metrics": [
                    {
                        "expressionType": "SQL",
                        "sqlExpression": "sum(metric)",
                        "column": None,
                        "aggregate": None,
                        "isNew": False,
                        "hasCustomLabel": True,
                        "label": "Value",
                        "optionName": "metric_ca8lx3tbdy9_72xmaamcbio",
                    }
                ],
                "granularity_sqla": "dt",
                "time_grain_sqla": time_grain_sqla,
                "adhoc_filters": [],
                "groupby": [],
                "limit": 1000,
                "row_limit": 1000,
                "where": "",
                "having": "",
                "series": None,
                "order_desc": True,
                "contribution": False,
                "show_brush": "no",
                "show_legend": True,
                "line_interpolation": "monotone",
                "rich_tooltip": True,
                "y_axis_format": "SMART_NUMBER",
                "show_markers": False,
                "x_axis_label": "Time",
                "x_axis_format": "smart_date",
                "bottom_margin": "auto",
                "x_ticks_layout": "auto",
                "x_axis_showminmax": False,
                "y_axis_bounds": [0, None],
                "y_log_scale": False,
                "rolling_type": None,
                "comparison_type": None,
                "resample_rule": None,
                "resample_method": None,
                "annotation_layers": [],
                "y_axis_label": "Metric Value",
                "x_axis_sort": "alpha_asc",
            }
        ),
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "owners": [SS_USER_ID],
    }

    with requests.Session() as session:
        session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        )

        csrf_url = f"{base_url}/api/v1/security/csrf_token/"
        csrf_response = session.get(csrf_url)
        csrf_response.raise_for_status()
        csrf_token = csrf_response.json().get("result")

        session.headers.update({"Referer": csrf_url, "X-CSRFToken": csrf_token})

        response = session.post(url, json=payload)
        if response.status_code != 201:
            logging.info(
                f"Failed to create dataset: {response.status_code} {response.reason}"
            )
            if len(response.content) < 300:
                logging.info(f"Response content: {response.content.decode()}")
            else:
                logging.info("Response content is too long to display.")

        response.raise_for_status()

        return response.json()


def generate_chart_link(slice_id: int) -> str:
    explore_path = "/superset/explore/"
    form_data = {"slice_id": slice_id}
    form_data_encoded = urlparse.quote(json.dumps(form_data))

    return f"{SS_URL}{explore_path}?form_data={form_data_encoded}"


def get_chart_id_by_name(
    base_url: str, token: str, chart_name: str, prefix: str = "MetricWatch: "
) -> int | None:
    """Fetches the chart ID by chart name."""
    url = f"{base_url}/api/v1/chart/"
    query = json.dumps(
        {
            "filters": [
                {"col": "slice_name", "opr": "eq", "value": prefix + chart_name}
            ],
            "order_column": "slice_name",
            "order_direction": "asc",
        }
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    response = requests.get(url, headers=headers, params={"q": query})
    response.raise_for_status()

    charts = response.json()

    if charts["count"] == 0:
        return None

    return charts["result"][0]["id"]


def main():
    """Main function to execute the dataset creation."""
    report_name = input("Enter the name of the report: ")
    granularity = report_name.split("_")[-2]

    try:
        access_token = get_access_token(SS_URL, SS_USER_NAME, SS_PASSWORD)
        create_dataset_response = create_dataset(
            base_url=SS_URL,
            token=access_token,
            db_id=int(SS_DATABASE_ID),
            schema="reports",
            table_name=report_name,
        )
        logging.info("Dataset created successfully:", create_dataset_response)

        create_chart_response = create_chart(
            base_url=SS_URL,
            token=access_token,
            dataset_id=create_dataset_response.get("id"),
            name=report_name,
            prefix="MetricWatch: ",
            granularity=granularity,
        )
        logging.info("Chart created successfully:", create_chart_response)

        chart_link = generate_chart_link(create_chart_response.get("id"))
        logging.info(f"Chart link: {chart_link}")
    except requests.exceptions.RequestException as err:
        logging.error(f"An error occurred: {err}")


if __name__ == "__main__":
    main()
