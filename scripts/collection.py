from binascii import Incomplete
import json
import os
from typing import Any, Callable
import requests

from dotenv import load_dotenv

from scripts.api import IngestionApi
from .utils import args_handler, get_collections


def map_collections(query: str, mapper: Callable[[IngestionApi, Any], None]):
    load_dotenv()
    files = get_collections(query)
    ingestor = IngestionApi.from_veda_auth_secret(
        secret_id=os.environ.get("COGNITO_APP_SECRET"),
        base_url=os.environ.get("STAC_INGESTOR_API_URL"),
    )
    for file in files:
        with open(file) as fd:
            mapper(ingestor, json.load(fd))


@args_handler
def insert(collections: str):
    print("Inserting collections:")

    session = requests.Session()

    def insert_one(api: IngestionApi, collection: Incomplete):
        try:
            response = api.request(
                "POST", "/collections", session=session, json=collection
            )
            response.raise_for_status()
            print(response.text)
        except:
            print(f"Error with {collection['id']}.")
            print(response.text)
            session.close()
            raise

    map_collections(collections, insert_one)
    session.close()


@args_handler
def delete(collections: str):
    print("Deleting collections")

    session = requests.Session()

    def delete_one(api: IngestionApi, collection: Incomplete):
        try:
            response = api.request(
                "DELETE",
                f"/collections/{collection['id']}",
                session=session,
            )
            print(response.text)
        except:
            print(f"Error with {collection['id']}.")
            print(response.json())
            session.close()
            raise

    map_collections(collections, delete_one)
    session.close()


@args_handler
def update(collections: str):
    print("Function not implemented")
