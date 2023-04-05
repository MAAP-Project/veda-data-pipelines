import json
import asyncio
import aiohttp
import json
import sys
from urllib.parse import urlencode

def parse_collection(collection):
    short_name = collection['short_name']
    dataset_id = collection['dataset_id']
    summary = collection['summary']
    boxes = collection['boxes']
    time_start = collection['time_start']
    time_end = collection['time_end']
    version_id = collection['version_id']

    bbox = [[float(coord) for coord in box.split()] for box in boxes]
    bbox = [[box[1], box[2], box[3], box[0]] for box in bbox]
    interval = [[time_start if time_start else None, time_end if time_end else None]]

    return [
        {
            'id': short_name,
            'stac_version': '1.0.0',
            'license': 'not-provided',
            'title': dataset_id,
            'type': 'Collection',
            'description': summary,
            'extent': {
                'spatial': {'bbox': bbox},
                'temporal': {'interval': interval}
            }
        },
        {
            'queue_messages': 'true',
            'collection': short_name,
            'version': version_id,
            'discovery': 'cmr',
            'mode': 'cmr',
            'asset_name': 'data',
            'asset_roles': ['data']
        }
    ]

async def fetch_collections(title):
    async with aiohttp.ClientSession() as session:
        headers = {
            'Accept': 'application/vnd.nasa.cmr.umm_results+json; version=1.10',
            'Accept-Language': 'en-US,en;q=0.5',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache'
        }
        body = urlencode({'keyword': title.replace(' ', '* ') + '*'})
        async with session.post('https://cmr.maap-project.org/search/collections.json', headers=headers, data=body) as resp:
            result = await resp.json()
            collections = {}
            for entry in result['feed']['entry']:
                col, item = parse_collection(entry)
                if col['id'] in collections:
                    collections[col['id']][1].append(item)
                else:
                    collections[col['id']] = [col, [item]]
            for name, (col, items) in collections.items():
                with open(f'data/collections/{name}.json', 'w') as f:
                    json.dump(col, f, indent=2)
                with open(f'data/step_function_inputs/{name}.json', 'w') as f:
                    json.dump(items[0] if len(items) == 1 else items, f, indent=2)

async def main():
    await fetch_collections(*sys.argv[1:])

if __name__ == '__main__':
    asyncio.run(main())

# python collection_script.py "Title of collection"
