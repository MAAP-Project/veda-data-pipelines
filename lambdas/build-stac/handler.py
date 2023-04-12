import json
import os
import importlib
import subprocess
import sys 
from typing import Any, Dict, TypedDict, Union
from uuid import uuid4

import smart_open

from utils import stac, events

AVAILABLE_STACTOOLS_MODULES = ["nisar_sim"] # update requirements.txt if this is updated.

class S3LinkOutput(TypedDict):
    stac_file_url: str


class StacItemOutput(TypedDict):
    stac_item: Dict[str, Any]


def handler(event: Dict[str, Any], context) -> Union[S3LinkOutput, StacItemOutput]:
    """
    Lambda handler for STAC Collection Item generation

    Arguments:
    event - object with event parameters to be provided in one of 3 formats.
        Format option 1 (with Granule ID defined to retrieve all metadata from CMR):
        {
            "collection": "OMDOAO3e",
            "remote_fileurl": "s3://climatedashboard-data/OMDOAO3e/OMI-Aura_L3-OMDOAO3e_2022m0120_v003-2022m0122t021759.he5.tif",
            "granule_id": "G2205784904-GES_DISC",
        }
        Format option 2 (with regex provided to parse datetime from the filename:
        {
            "collection": "OMDOAO3e",
            "remote_fileurl": "s3://climatedashboard-data/OMSO2PCA/OMSO2PCA_LUT_SCD_2005.tif",
        }
        Format option 3 (with stactools package) where stactools-module represents the module of the stactools package to use. The package must comply with https://github.com/stactools-packages/template and be installed in the instance executing this code. 
        {
            "collection" : "NISAR",
            "stactools-module": "<stactools-module",
            " ... additional keys corresponding to the module's `create_item` method arguments"
        }

        example : 

        {
            "collection": "NISAR",
            "stactools-package": "nisar_sim",
            "source":"tests/data-files/winnip_31604_12061_004_120717_L090_CX_07",
            "dither": "X"
        }


    """

    if "stactools-module" in event:
        module_str = event.pop("stactools-module")
        assert module_str in AVAILABLE_STACTOOLS_MODULES
        stac_module = importlib.import_module(f'stactools.{module_str}.stac')
        stac_item = stac_module.create_item(event)
    else:
        EventType = events.CmrEvent if event.get("granule_id") else events.RegexEvent
        parsed_event = EventType.parse_obj(event)
        stac_item = stac.generate_stac(parsed_event).to_dict()

    output: StacItemOutput = {"stac_item": stac_item}

    # Return STAC Item Directly
    if sys.getsizeof(json.dumps(output)) < (256 * 1024):
        return output

    # Return link to STAC Item
    key = f"s3://{os.environ['BUCKET']}/{uuid4()}.json"
    with smart_open.open(key, "w") as file:
        file.write(json.dumps(stac_item))

    return {"stac_file_url": key}


if __name__ == "__main__":
    
    # sample_event = {
    #     "collection": "GEDI02_A",
    #     "remote_fileurl": "s3://nasa-maap-data-store/file-staging/nasa-map/GEDI02_A___002/2020.12.31/GEDI02_A_2020366232302_O11636_02_T08595_02_003_02_V002.h5",
    #     "granule_id": "G1201782029-NASA_MAAP",
    #     "id": "G1201782029-NASA_MAAP",
    #     "mode": "cmr",
    #     "test_links": None,
    #     "reverse_coords": None,
    #     "asset_name": "data",
    #     "asset_roles": ["data"],
    #     "asset_media_type": "application/x-hdf5",
    # }

    sample_event = {
        "collection": "NISAR",
        "stactools-module": "nisar_sim",
        "source":"tests/data-files/winnip_31604_12061_004_120717_L090_CX_07",
        "dither": "X"
    }

    print(json.dumps(handler(sample_event, {}), indent=2))
