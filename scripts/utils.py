from sys import argv
import functools
import glob
import os
import boto3


DATA_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data")


def data_files(data, data_path):
    files = []
    for item in data:
        files.extend(glob.glob(os.path.join(data_path, f"{item}*.json")))
    return files


def get_items(query):
    items_path = os.path.join(DATA_PATH, "step_function_inputs")
    return data_files(query, items_path)


def get_collections(query):
    collections_path = os.path.join(DATA_PATH, "collections")
    return data_files(query, collections_path)


def arguments():
    if len(argv) <= 1:
        print("No collection provided")
        return
    return argv[1:]


def args_handler(func):
    @functools.wraps(func)
    def prep_args(*args, **kwargs):
        internal_args = arguments()
        func(internal_args)

    return prep_args


def get_discovery_lambda_arn():
    sts = boto3.client("sts")
    ACCOUNT_ID = sts.get_caller_identity().get("Account")
    REGION = os.environ.get("AWS_REGION", "us-west-2")
    APP_NAME = os.environ.get("APP_NAME")
    ENV = os.environ.get("ENV", "dev")
    return f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{APP_NAME}-{ENV}-lambda-trigger-discover-fn"
