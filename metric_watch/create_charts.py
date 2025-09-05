from os import getenv as env, listdir
import logging
import requests
from metric_watch.charts import (
    create_dataset,
    create_chart,
    get_access_token,
    SS_URL,
    SS_DATABASE_ID,
    SS_USER_NAME,
    SS_PASSWORD,
)


required_env_vars = ["SS_URL", "SS_USER_NAME", "SS_PASSWORD", "SS_DATABASE_ID"]
missing_env_vars = [var for var in required_env_vars if not env(var)]
if missing_env_vars:
    raise EnvironmentError(
        f"Missing required environment variables: {', '.join(missing_env_vars)}"
    )

logging.basicConfig(level=logging.INFO)


def create_dataset_and_chart(name: str) -> None:
    token = get_access_token(SS_URL, SS_USER_NAME, SS_PASSWORD)
    granularity = name.split("_")[-2]
    create_dataset_response = None

    try:
        create_dataset_response = create_dataset(
            SS_URL,
            token,
            SS_DATABASE_ID,
            "reports",
            name,
        )
    except requests.exceptions.RequestException as err:
        logging.error(f"An error occurred: {err}")
    else:
        logging.info(f"Dataset for {name} was successfully created")

    if create_dataset_response:
        try:
            create_chart(
                base_url=SS_URL,
                token=token,
                dataset_id=create_dataset_response.get("id"),
                name=name,
                prefix="MetricWatch: ",
                granularity=granularity,
            )
        except requests.exceptions.RequestException as err:
            logging.error(f"An error occurred: {err}")
        else:
            logging.info(f"Chart for {name} were successfully created")


def run():
    for file in listdir("artefacts"):
        table_name = file.split(".")[0].lower()
        create_dataset_and_chart(table_name)
