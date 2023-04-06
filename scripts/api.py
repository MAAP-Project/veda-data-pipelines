import base64
from binascii import Incomplete
from dataclasses import dataclass
import json
from typing import TypedDict, Union

import boto3
import requests


class AppConfig(TypedDict):
    cognito_domain: str
    client_id: str
    client_secret: str
    scope: str


class Creds(TypedDict):
    access_token: str
    expires_in: int
    token_type: str


@dataclass
class IngestionApi:
    base_url: str
    token: str

    @classmethod
    def from_veda_auth_secret(cls, *, secret_id: str, base_url: str) -> "IngestionApi":
        cognito_details = cls._get_cognito_service_details(secret_id)
        credentials = cls._get_app_credentials(**cognito_details)
        return cls(token=credentials["access_token"], base_url=base_url)

    @staticmethod
    def _get_cognito_service_details(secret_id: str) -> AppConfig:
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_id)
        if "SecretString" in response:
            return json.loads(response["SecretString"])
        else:
            return json.loads(base64.b64decode(response["SecretBinary"]))

    @staticmethod
    def _get_app_credentials(
        cognito_domain: str, client_id: str, client_secret: str, scope: str, **kwargs
    ) -> Creds:
        response = requests.post(
            f"{cognito_domain}/oauth2/token",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
            },
            auth=(client_id, client_secret),
            data={
                "grant_type": "client_credentials",
                # A space-separated list of scopes to request for the generated access token.
                "scope": scope,
            },
        )
        try:
            response.raise_for_status()
        except:
            print(response.text)
            raise
        return response.json()

    def request(
        self,
        method: str,
        path: str,
        session: Union[requests.Session, None] = None,
        json: Union[Incomplete, None] = None,
    ):
        if not session:
            session = requests

        return session.request(
            method,
            f"{self.base_url.rstrip('/')}/{path.lstrip('/')}",
            json=json,
            headers={"Authorization": f"bearer {self.token}"},
        )
