from clickhouse_driver import Client  # type: ignore
from collections import deque
import os

# Set ClickHouse connection
password = os.getenv("CH_PASSWORD")
user = os.getenv("CH_USER")
host = os.getenv("CLICKHOUSE_HOST_IP")

ch_client = Client(
    host=host,
    user=user,
    password=password,
)

DIR = "artefacts"


def create():
    queue = deque(os.listdir(DIR))

    # TODO: refactor potential infinite loop
    limit = 100
    counter = 0

    while queue:
        if counter >= limit:
            print("INFO: Limit is reached, breaking the loop")
            break
        file = queue.popleft()

        with open(f"{DIR}/{file}", "r") as f:
            content = f.read()
            if content.startswith("CREATE OR REPLACE VIEW"):
                try:
                    ch_client.execute(content)
                except Exception as e:
                    counter += 1
                    print(f"View creation failed: {file}")
                    print(e)
                    queue.append(file)
                else:
                    print(f"View created: {file}")


def drop():
    for file in os.listdir(DIR):
        with open(f"{DIR}/{file}", "r") as f:
            content = f.read()
            if content.startswith("CREATE OR REPLACE VIEW"):
                ch_client.execute(
                    content.split("AS")[0].replace(
                        "CREATE OR REPLACE VIEW", "DROP VIEW IF EXISTS"
                    )
                )
                print(f"View dropped if existed: {file}")
