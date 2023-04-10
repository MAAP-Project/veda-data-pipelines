from collections import defaultdict
import os
import json
import re
import boto3

from uuid import uuid4

INVALID_NAME_CHARS = re.compile("[^a-zA-Z0-9_-]")


def filter_sfname(name):
    if name is None:
        return ""
    return re.sub(INVALID_NAME_CHARS, "", name)


def group_by_collection(records):
    collections = defaultdict(list)
    for record in records:
        collections[record.get("collection", None)].append(record)
    return collections


def handler(event, context):
    STEP_FUNCTION_ARN = os.environ["STEP_FUNCTION_ARN"]
    step_function_input = (json.loads(record["body"]) for record in event["Records"])

    client = boto3.client("stepfunctions")
    for collection, records in group_by_collection(step_function_input).items():
        name = filter_sfname(collection)
        client.start_execution(
            name=f"{name[:40]}-{str(uuid4())}",
            stateMachineArn=STEP_FUNCTION_ARN,
            input=json.dumps(records),
        )
    return
