import json
import os
from sys import getsizeof
from typing import Any, Dict, TypedDict, Union
from uuid import uuid4

import smart_open

from utils import stac, events


class S3LinkOutput(TypedDict):
    stac_file_url: str


class StacItemOutput(TypedDict):
    stac_item: Dict[str, Any]


def handler(event: Dict[str, Any], context) -> Union[S3LinkOutput, StacItemOutput]:
    """
    Lambda handler for STAC Collection Item generation

    Arguments:
    event - object with event parameters.
        {
            "collection": "OMDOAO3e",
            "remote_fileurl": "s3://climatedashboard-data/OMSO2PCA/OMSO2PCA_LUT_SCD_2005.tif",
        }

    """

    parsed_event = events.RegexEvent.parse_obj(event)
    stac_item = stac.generate_stac(parsed_event).to_dict()

    output: StacItemOutput = {"stac_item": stac_item}

    # Return STAC Item Directly
    if getsizeof(json.dumps(output)) < (256 * 1024):
        return output

    # Return link to STAC Item
    key = f"s3://{os.environ['BUCKET']}/{uuid4()}.json"
    with smart_open.open(key, "w") as file:
        file.write(json.dumps(stac_item))

    return {"stac_file_url": key}


if __name__ == "__main__":
    sample_event = {
        "collection": "icesat2-boreal",
        "remote_fileurl": "s3://maap-user-shared-data/icesat2-boreal/boreal_agb_202302151676439579_1326.tif",
        "upload": True,
        "properties": {}
    }
    print(json.dumps(handler(sample_event, {}), indent=2))
