import json
import os
from functools import singledispatch
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import geojson
import pystac
import rasterio
import requests
from cmr import GranuleQuery
from pystac.utils import str_to_datetime
from rasterio.session import AWSSession
from rio_stac import stac

from . import events, regex, role


def create_item(
    id,
    properties,
    links,
    datetime,
    item_url,
    collection,
    mode=None,
    bbox=None,
    geometry=None,
    assets=None,
    asset_name=None,
    asset_roles=None,
    asset_media_type=None,
) -> pystac.Item:
    """
    Function to create a stac item from a COG using rio_stac
    """

    def create_item_item():
        stac_item = pystac.Item(
            id=id,
            geometry=geometry,
            properties=properties,
            href=item_url,
            datetime=datetime,
            collection=collection,
            bbox=bbox,
        )
        stac_item.links.extend(links)
        stac_item.assets = assets
        return stac_item

    if mode == "cmr":
        return create_item_item()

    rasterio_kwargs = {}
    if role_arn := os.environ.get("DATA_MANAGEMENT_ROLE_ARN"):
        creds = role.assume_role(role_arn, "veda-data-pipelines_build-stac")
        rasterio_kwargs["session"] = AWSSession(
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
        )

    with rasterio.Env(
        session=rasterio_kwargs.get("session"),
        options={
            **rasterio_kwargs,
            "GDAL_MAX_DATASET_POOL_SIZE": 1024,
            "GDAL_DISABLE_READDIR_ON_OPEN": False,
            "GDAL_CACHEMAX": 1024000000,
            "GDAL_HTTP_MAX_RETRY": 4,
            "GDAL_HTTP_RETRY_DELAY": 1,
        },
    ):
        try:
            # `stac.create_stac_item` tries to opon a dataset with rasterio.
            # if that fails (since not all items are rasterio-readable), fall back to pystac.Item
            return stac.create_stac_item(
                id=id,
                source=item_url,
                collection=collection,
                input_datetime=datetime,
                properties=properties,
                with_proj=True,
                with_raster=True,
                assets=assets,
                asset_name=asset_name or "cog_default",
                asset_roles=asset_roles or ["data", "layer"],
                asset_media_type=(
                    asset_media_type
                    or "image/tiff; application=geotiff; profile=cloud-optimized"
                ),
            )
        except Exception as e:
            print(f"Caught exception {e}")
            if "not recognized as a supported file format" in str(e):
                return create_item_item()
            else:
                raise


@singledispatch
def generate_stac(item) -> pystac.Item:
    """
    Generate STAC item from user provided datetime range or regex & filename
    """
    raise NotImplementedError(f"Cannot generate STAC for {type(item)=}, {item=}")


@generate_stac.register
def generate_stac_regexevent(item: events.RegexEvent) -> pystac.Item:
    """
    Generate STAC item from user provided datetime range or regex & filename
    """
    if item.start_datetime and item.end_datetime:
        start_datetime = item.start_datetime
        end_datetime = item.end_datetime
        single_datetime = None
    elif single_datetime := item.single_datetime:
        start_datetime = end_datetime = None
        single_datetime = single_datetime
    else:
        start_datetime, end_datetime, single_datetime = regex.extract_dates(
            item.remote_fileurl, item.datetime_range
        )
    properties = item.properties or {}
    if start_datetime and end_datetime:
        # these are added post-serialization to properties, unlike single_datetime
        properties["start_datetime"] = start_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
        properties["end_datetime"] = end_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
        single_datetime = None

    return create_item(
        id=item.item_id(),
        properties=properties,
        datetime=single_datetime,
        item_url=item.remote_fileurl,
        collection=item.collection,
        asset_name=item.asset_name,
        asset_roles=item.asset_roles,
        asset_media_type=item.asset_media_type,
    )


def pairwise(iterable) -> zip:
    """
    generates a generator object of tuples from a flat list
    "[s0, s1, s2, s3, s4, s5, ...] -> [(s0, s1), (s2, s3), (s4, s5), ...]"
    """
    a = iter(iterable)
    return zip(a, a)


def get_bbox(coord_list) -> list[float]:
    """
    Returns the corners of a list of coordinates by:
    1. Sorting the coordinates by latitude and longitude coordinates
    2. Adding the min and max value for latitude and min and max value for longitude to a list
    3. Returning a list of min x, min y, max x, max y
    """
    box = []
    for i in (0, 1):
        res = sorted(coord_list, key=lambda x: x[i])
        box.append((res[0][i], res[-1][i]))
    return [box[0][0], box[1][0], box[0][1], box[1][1]]


def generate_geometry_from_cmr(polygons, boxes, reverse_coords) -> dict:
    """
    Generates geoJSON object from list of coordinates provided in CMR JSON
    """
    str_coords = None
    if polygons:
        str_coords = polygons[0][0].split()
        if reverse_coords:
            str_coords.reverse()
    elif boxes:
        str_coords = boxes[0].split()

    if not str_coords:
        return None
    polygon_coords = [(float(x), float(y)) for x, y in pairwise(str_coords)]
    if len(polygon_coords) == 2:
        polygon_coords.insert(1, (polygon_coords[1][0], polygon_coords[0][1]))
        polygon_coords.insert(3, (polygon_coords[0][0], polygon_coords[2][1]))
        polygon_coords.insert(4, polygon_coords[0])
    return {"coordinates": [polygon_coords], "type": "Polygon"}


def _content_type(link: str, asset_media_type: Union[str, dict]) -> str:
    if isinstance(asset_media_type, dict):
        file = Path(link)
        return asset_media_type.get(
            file.suffix, asset_media_type.get(file.suffix[1:], None)
        )
    else:
        return asset_media_type


def _roles(link: str, asset_roles: Union[list, dict], default: List[str]) -> List[str]:
    if isinstance(asset_roles, dict):
        file = Path(link)
        return asset_roles.get(file.suffix, asset_roles.get(file.suffix[1:], default))
    else:
        return asset_roles


def generate_asset(
    roles: Union[str, Dict[str, List[str]]],
    link: dict,
    item: dict,
    default_role: list = None,
) -> pystac.Asset:
    href = link.get("href")
    if item.test_links and "http" in href:
        try:
            requests.head(href).raise_for_status()
        except Exception as e:
            print(f"Caught error for link {link}: {e}")
            return None

    # If type is in CMR link{} use that, else use the type from the asset_media_type
    asset_media_type = link.get("type", _content_type(href, item.asset_media_type))
    asset_roles = _roles(href, roles, default_role or ["data"])

    return pystac.Asset(roles=asset_roles, href=href, media_type=asset_media_type)


def generate_link(
    rel: Union[str, pystac.RelType],
    target: Union[str, pystac.STACObject],
    media_type: Optional[str] = None,
    title: Optional[str] = None,
    extra_fields: Optional[Dict[str, Any]] = None,
) -> pystac.Link:
    """
    Generate a link object from a rel, target, media type, title, and extra fields.
    """
    return pystac.Link(
        rel=rel,
        target=target,
        media_type=media_type,
        title=title,
        extra_fields=extra_fields,
    )


def from_cmr_links(cmr_links, item) -> Tuple[List, Dict[str, pystac.Asset]]:
    """
    Generates a dictionary of pystac.Asset's from cmr_json links
    """
    assets = {}
    links = []
    for link in cmr_links:
        if link["rel"].endswith("data#"):
            extension = os.path.splitext(link["href"])[-1].replace(".", "")
            if extension == "prj":
                asset = generate_asset(
                    item.asset_roles, link, item, default_role=["metadata"]
                )
            if (
                asset := generate_asset(item.asset_roles, link, item)
            ) and "data" not in assets:
                assets["data"] = asset
        if link["rel"].endswith("s3#"):
            if asset := generate_asset(item.asset_roles, link, item):
                assets["data"] = asset
        if link["rel"].endswith("metadata#"):
            links.append(
                generate_link(
                    "metadata", link["href"], link.get("type"), link.get("title")
                )
            )
        if link["rel"].endswith("documentation#"):
            links.append(
                generate_link(
                    "documentation", link["href"], link.get("type"), link.get("title")
                )
            )

    if item.assets:
        # Removes the default data asset, exists as a duplicate
        del assets["data"]

        pystac_asset = lambda link: pystac.Asset(
            roles=_roles(link, item.asset_roles, ["data"]),
            href=link,
            media_type=_content_type(link, item.asset_media_type),
        )
        pystac_assets = {key: pystac_asset(value) for key, value in item.assets.items()}
        return links, dict(sorted((pystac_assets | assets).items()))

    return links, assets


def cmr_api_url() -> str:
    default_cmr_api_url = (
        "https://cmr.maap-project.org"  # "https://cmr.earthdata.nasa.gov"
    )
    return os.environ.get("CMR_API_URL", default_cmr_api_url)


@generate_stac.register
def generate_stac_cmrevent(item: events.CmrEvent) -> pystac.Item:
    """
    Generates a STAC Item from a CmrEvent
    """
    properties = (
        GranuleQuery(mode=f"{cmr_api_url()}/search/")
        .concept_id(item.granule_id)
        .get(1)[0]
    )
    properties["concept_id"] = properties.pop("id")
    del properties["title"]  # Remove title from properties, it's already in the item

    geometry = generate_geometry_from_cmr(
        properties.pop("polygons", None),
        properties.pop("boxes", None),
        item.reverse_coords,
    )

    if geometry:
        bbox = get_bbox(list(geojson.utils.coords(geometry["coordinates"])))
    else:
        bbox = None

    links, assets = from_cmr_links(properties.pop("links", None), item)

    return create_item(
        id=item.item_id(),
        properties=properties,
        links=links,
        mode=item.mode,
        datetime=str_to_datetime(properties["time_start"]),
        item_url=item.remote_fileurl,
        collection=item.collection,
        asset_name=item.asset_name,
        asset_roles=item.asset_roles,
        asset_media_type=item.asset_media_type,
        assets=assets,
        bbox=bbox,
        geometry=geometry,
    )
