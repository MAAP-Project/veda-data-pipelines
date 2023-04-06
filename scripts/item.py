import json
import boto3

from .utils import args_handler, get_items, get_discovery_lambda_arn


def insert_items(files):
    print("Inserting items:")
    lambda_client = boto3.client("lambda")
    for filename in files:
        print(filename)
        with open(filename) as fd:
            events = json.load(fd)
            if type(events) != list:
                events = [events]
            for event in events:
                response = lambda_client.invoke(
                    FunctionName=get_discovery_lambda_arn(),
                    InvocationType="Event",
                    Payload=json.dumps(event),
                )
                print(response)


@args_handler
def insert(items):
    insert_items(get_items(items))


def update(items):
    print("Function not implemented")


def delete(items):
    print("Function not implemented")
