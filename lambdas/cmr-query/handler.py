import json
import os
import re
from typing import Any, Dict, List
from urllib.parse import urlparse
import requests


def multi_asset_items(
    data_file: str, data_file_regex: str, data: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Returns a list of file_obj's with the added "assets" key:value where "assets"
    is a Dict[str, Any] used to add item assets to a STAC item

    Parameters:
        data_file: str
            string value describing the data file from which to build the STAC item
        data_file_regex: str
            string value becomes a regex pattern to find all related data file urls,
            commonly a product ID or other identifier shared amongst product files
        data: List[Dict[str, Any]]
            A list of dictionary file_obj's generated from querying CMR
    Return:
        objects: List[Dict[str, Any]]
            A list of modified file_obj dictionaries, used to generate STAC items

    Example:
        multi_asset_items(
            "cov_1-1.hdr".
            "uavsar_AfriSAR_v1-.*_.{5}_.{5}_.{3}_.{3}_.{6}",
            [
                {
                    'collection': 'AfriSAR_UAVSAR_Ungeocoded_Covariance',
                    'remote_fileurl': 's3://nasa-maap-data-store/file-staging/nasa-map/AfriSAR_UAVSAR_Ungeocoded_Covariance___1/uavsar_AfriSAR_v1-cov_topo_a41_r9_localPsi.rdr',
                    'granule_id': 'G1200110696-NASA_MAAP',
                    'id': 'G1200110696-NASA_MAAP',
                    'mode': 'cmr',
                    'test_links': None,
                    'reverse_coords': None,
                    'asset_name': 'data',
                    'asset_roles': ['data'],
                    'asset_media_type': {
                        'vrt': 'application/octet-stream',
                        'bin': 'application/octet-stream',
                        'hdr': 'binary/octet-stream'
                    },
                }
            ]
        )
        Returns:
        [
            {
                'collection': 'AfriSAR_UAVSAR_Ungeocoded_Covariance',
                'remote_fileurl': 's3://nasa-maap-data-store/file-staging/nasa-map/AfriSAR_UAVSAR_Ungeocoded_Covariance___1/uavsar_AfriSAR_v1-cov_coreg_fine_hsixty_14050_16015_140_009_160308_cov_1-1.hdr',
                'granule_id': 'G1200109928-NASA_MAAP',
                'id': 'G1200109928-NASA_MAAP',
                'mode': 'cmr',
                'test_links': None,
                'reverse_coords': None,
                'asset_name': 'data',
                'asset_roles': ['data'],
                'asset_media_type': 'application/x-hdr',
                'assets': {
                    'cov_1-1.bin': 's3://nasa-maap-data-store/file-staging/nasa-map/AfriSAR_UAVSAR_Ungeocoded_Covariance___1/uavsar_AfriSAR_v1-cov_coreg_fine_hsixty_14050_16015_140_009_160308_cov_1-1.bin',
                    'cov_1-1.hdr': 's3://nasa-maap-data-store/file-staging/nasa-map/AfriSAR_UAVSAR_Ungeocoded_Covariance___1/uavsar_AfriSAR_v1-cov_coreg_fine_hsixty_14050_16015_140_009_160308_cov_1-1.hdr',
                    'cov_1-2.bin': 's3://nasa-maap-data-store/file-staging/nasa-map/AfriSAR_UAVSAR_Ungeocoded_Covariance___1/uavsar_AfriSAR_v1-cov_coreg_fine_hsixty_14050_16015_140_009_160308_cov_1-2.bin',
                    ...,
                }
                'product_id': 'uavsar_AfriSAR_v1-cov_coreg_fine_hsixty_14050_16015_140_009_160308'
            }
        ]
    """
    fileurls_pattern = re.compile(data_file_regex)
    objects = []
    product_ids = {}

    def _get_asset_name(remote_fileurl: str, product_id: str) -> str:
        return re.sub(f".*{product_id}[-_.]?", "", remote_fileurl)

    # Creates a Dict[product_id, Dict[file_name, List[str]]]
    for item in data:
        match = re.search(fileurls_pattern, item["remote_fileurl"])
        if match:
            product_id = match.group()
            product_ids[product_id] = product_ids.get(product_id, {})

            product_ids[product_id][
                _get_asset_name(item["remote_fileurl"], product_id)
            ] = item["remote_fileurl"]

    # Creates an objects Dict of modified file_obj's, adding file_obj["assets"]
    for product_id in product_ids.keys():
        for file_obj in data:
            if re.search(f".*{product_id}.*{data_file}", file_obj["remote_fileurl"]):
                file_obj["assets"] = dict(sorted(product_ids[product_id].items()))
                file_obj["product_id"] = product_id
                objects.append(file_obj)

    return objects


def get_cmr_granules_endpoint(event):
    default_cmr_api_url = (
        "https://cmr.maap-project.org"  # "https://cmr.earthdata.nasa.gov"
    )
    cmr_api_url = event.get(
        "cmr_api_url", os.environ.get("CMR_API_URL", default_cmr_api_url)
    )
    cmr_granules_search_url = f"{cmr_api_url}/search/granules.json"
    return cmr_granules_search_url


def default_granules(event, granules):
    link_rel = event.get("link_rel", "http://esipfed.org/ns/fedsearch/1.1/s3#")

    # don't overwrite the fileurl if it's already been discovered.
    event_defaults = {
        "collection": event["collection"],
        "mode": event.get("mode"),
        "test_links": event.get("test_links"),
        "reverse_coords": event.get("reverse_coords"),
        ** { key: value for key, value in event.items() if "asset" in key }
    }

    for granule in granules:
        granule_defaults = {
            "granule_id": granule["id"],
            "id": granule["id"],
        }
        link = next(
            filter(lambda l: l["rel"] == link_rel, granule['links']), 
            None
        )
        if link is None:
            continue
        yield {
            ** event_defaults,
            ** granule_defaults,
            "remote_fileurl": link["href"],
        }


def stac_granules(granules):
    def is_stac_link(link):
        url = urlparse(link)
        return url.path[-9:] == "stac.json" and url.scheme == "https"

    return (link for granule in granules for link in granule["links"] if is_stac_link(link))


def handler(event, context):
    """
    Lambda handler for the NetCDF ingestion pipeline
    """
    collection = event["collection"]
    version = event["version"]

    temporal = event.get("temporal", ["1000-01-01T00:00:00Z", "3000-01-01T23:59:59Z"])
    page = event.get("start_after", 1)
    limit = event.get("limit", 100)

    search_endpoint = (
        f"{get_cmr_granules_endpoint(event)}?short_name={collection}&version={version}"
        + f"&temporal[]={temporal[0]},{temporal[1]}&page_size={limit}"
    )
    search_endpoint = f"{search_endpoint}&page_num={page}"
    print(f"Discovering data from {search_endpoint}")
    response = requests.get(search_endpoint)

    if response.status_code != 200:
        print(f"Got an error from CMR: {response.status_code} - {response.text}")
        return

    hits = response.headers["CMR-Hits"]
    print(f"Got {hits} from CMR")
    granules = json.loads(response.text)["feed"]["entry"]
    print(f"Got {len(granules)} to insert")
    # Decide if we should continue after this page
    # Start paging if there are more hits than the limit
    # Stop paging when there are no more results to return
    if len(granules) > 0 and int(hits) > limit * page:
        print(f"Got {int(hits)} which is greater than {limit*page}")
        event["start_after"] = page + 1
        print(f"Returning next page {event.get('start_after')}")
    else:
        event.pop("start_after", None)

    granules_to_insert = list(stac_granules(granules) if event.get("mode") == "stac" else default_granules(event, granules))
    if event.get("data_file_regex"):
        granules_to_insert = multi_asset_items(
            data_file=event.get("data_file"),
            data_file_regex=event.get("data_file_regex"),
            data=granules_to_insert,
        )

    return {
        **event,
        "cogify": event.get("cogify", False),
        "objects": granules_to_insert,
    }


if __name__ == "__main__":
    sample_event = {
        "queue_messages": "true",
        "collection": "AfriSAR_UAVSAR_KZ",
        "version": "1",
        "discovery": "cmr",
        "temporal": ["2016-02-25T00:00:00Z", "2016-03-08T00:00:00Z"],
        "mode": "cmr",
        "asset_name": "data",
        "asset_roles": ["data"],
        "asset_media_type": {
            "vrt": "application/octet-stream",
            "bin": "binary/octet-stream",
            "hdr": "binary/octet-stream",
        },
        "data_file": "hdr",
        "data_file_regex": "uavsar_AfriSAR_v1-.*.{5}_.{5}_.{3}_.{3}_.{6}_kz",
    }
    print(json.dumps(handler(sample_event, {}), indent=4))
