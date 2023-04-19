import os
from unittest import mock

import boto3
import pystac
import pytest
from moto import mock_s3
from mypy_boto3_s3.service_resource import Bucket, S3ServiceResource
from utils import events


@pytest.fixture(scope="session", autouse=True)
def mock_environment():
    with mock.patch.dict(os.environ, {"BUCKET": "test-bucket"}):
        yield os.environ


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def s3_client(aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def s3_resource(aws_credentials) -> S3ServiceResource:
    with mock_s3():
        yield boto3.resource("s3", region_name="us-east-1")


@pytest.fixture
def s3_created_bucket(s3_resource, mock_environment) -> Bucket:
    s3_bucket = s3_resource.Bucket(mock_environment["BUCKET"])

    s3_bucket.create()
    yield s3_bucket


@pytest.fixture
def sample_assets():
    return {
        "bin": "s3://nasa-maap-data-store/file-staging/nasa-map/AfriSAR_UAVSAR_KZ___1/uavsar_AfriSAR_v1-coreg_fine_lopenp_14043_16008_140_009_160225_kz.bin",
        "hdr": "s3://nasa-maap-data-store/file-staging/nasa-map/AfriSAR_UAVSAR_KZ___1/uavsar_AfriSAR_v1-coreg_fine_lopenp_14043_16008_140_009_160225_kz.hdr",
        "vrt": "s3://nasa-maap-data-store/file-staging/nasa-map/AfriSAR_UAVSAR_KZ___1/uavsar_AfriSAR_v1-coreg_fine_lopenp_14043_16008_140_009_160225_kz.vrt",
    }


@pytest.fixture
def cmr_multi_asset_sample_event(sample_assets):
    return events.CmrEvent(
        **{
            "collection": "AfriSAR_UAVSAR_KZ",
            "remote_fileurl": "s3://nasa-maap-data-store/file-staging/nasa-map/AfriSAR_UAVSAR_KZ___1/uavsar_AfriSAR_v1-coreg_fine_lopenp_14043_16008_140_009_160225_kz.hdr",
            "granule_id": "G1200110083-NASA_MAAP",
            "id": "G1200110083-NASA_MAAP",
            "mode": "cmr",
            "test_links": None,
            "reverse_coords": None,
            "asset_name": "data",
            "asset_roles": ["data"],
            "asset_media_type": "application/x-hdr",
            "assets": sample_assets,
            "product_id": "uavsar_AfriSAR_v1-coreg_fine_lopenp_14043_16008_140_009_160225_kz",
        }
    )


@pytest.fixture
def cmr_json_example():
    return {
        "boxes": ["-2.0677778 9.1694444 0.61 11.8641667"],
        "time_start": "2016-02-25T00:00:00.000Z",
        "updated": "2019-04-12T20:18:18.461Z",
        "dataset_id": "AfriSAR UAVSAR Vertical Wavenumber (KZ) Generated Using NISAR Tools",
        "data_center": "NASA_MAAP",
        "title": "uavsar_AfriSAR_v1-coreg_fine_lopenp_14043_16008_140_009_160225_kz.hdr",
        "coordinate_system": "CARTESIAN",
        "day_night_flag": "UNSPECIFIED",
        "time_end": "2016-02-25T23:59:59.000Z",
        "id": "G1200110083-NASA_MAAP",
        "original_format": "ECHO10",
        "browse_flag": False,
        "collection_concept_id": "C1200109243-NASA_MAAP",
        "online_access_flag": False,
        "links": [
            {
                "rel": "http://esipfed.org/ns/fedsearch/1.1/s3#",
                "title": "File to download",
                "hreflang": "en-US",
                "href": "s3://nasa-maap-data-store/file-staging/nasa-map/AfriSAR_UAVSAR_KZ___1/uavsar_AfriSAR_v1-coreg_fine_lopenp_14043_16008_140_009_160225_kz.hdr",
            },
            {
                "inherited": True,
                "rel": "http://esipfed.org/ns/fedsearch/1.1/data#",
                "hreflang": "en-US",
                "href": "s3://nasa-maap-data-store/file-staging/nasa-map/AfriSAR_UAVSAR_KZ___1",
            },
            {
                "inherited": True,
                "rel": "http://esipfed.org/ns/fedsearch/1.1/documentation#",
                "hreflang": "en-US",
                "href": "https://ieeexplore.ieee.org/document/8469014",
            },
        ],
    }
