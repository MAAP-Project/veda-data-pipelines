import json
from operator import itemgetter
import os
from typing import List
from dotenv import load_dotenv
import requests

from scripts.api import IngestionApi
from .utils import args_handler


def list():
    load_dotenv()
    ingestor = IngestionApi.from_veda_auth_secret(
        secret_id=os.environ.get("COGNITO_APP_SECRET"),
        base_url=os.environ.get("STAC_INGESTOR_API_URL"),
    )
    try:
        response = ingestor.request(
            "GET",
            "/ingestions",
        )
        response.raise_for_status()
        print(json.dumps(response.json(), indent=2))
    except:
        print(response.text)
        raise


@args_handler
def get(ids: List[str]):
    load_dotenv()
    ingestor = IngestionApi.from_veda_auth_secret(
        secret_id=os.environ.get("COGNITO_APP_SECRET"),
        base_url=os.environ.get("STAC_INGESTOR_API_URL"),
    )
    session = requests.Session()

    for id in ids:
        try:
            response = ingestor.request("GET", f"/ingestions/{id}", session=session)
            response.raise_for_status()
            id, status, message = itemgetter("id", "status", "message")(response.json())
            print(f"ingestion {id}: {status}")
            print(message)
        except:
            session.close()
            print(response.text)
            raise

    session.close()


@args_handler
def delete(ids: List[str]):
    load_dotenv()
    ingestor = IngestionApi.from_veda_auth_secret(
        secret_id=os.environ.get("COGNITO_APP_SECRET"),
        base_url=os.environ.get("STAC_INGESTOR_API_URL"),
    )
    session = requests.Session()

    for id in ids:
        try:
            response = ingestor.request(
                "DELETE",
                f"/ingestions/{id}",
            )
            response.raise_for_status()
            print(json.dumps(response.json(), indent=2))
        except:
            session.close()
            print(response.text)
            raise

    session.close()
