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


def handler(event, context):
    STEP_FUNCTION_ARN = os.environ["STEP_FUNCTION_ARN"]

    event.pop("objects", None)
    page = min(event.get("start_after", 1), 9999)
    name = filter_sfname(event.get("collection", None))

    client = boto3.client("stepfunctions")
    client.start_execution(
        name=f"{name[:38]}[{page:04d}]{str(uuid4())}",
        stateMachineArn=STEP_FUNCTION_ARN,
        input=json.dumps(event),
    )
    return
