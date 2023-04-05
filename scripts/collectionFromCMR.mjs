import { writeFile } from 'node:fs/promises'
import { argv } from 'node:process'

function parseCollection(collection) {
  const {
    short_name,
    dataset_id,
    summary,
    boxes,
    time_start,
    time_end,
    version_id,
  } = collection
  const bbox = boxes.map((str) => {
    const [south, west, north, east]= str.split(' ')
    return [ west, north, east, south ]
  })
  const interval = [[ time_start ?? null, time_end ?? null ]]
  return [ 
    {
      id: short_name,
      stac_version: "1.0.0",
      license: "not-provided",
      title: dataset_id,
      type: "Collection",
      description: summary,
      extent: { 
        spatial: { bbox },
        temporal: { interval },
      }
    },
    {
      "queue_messages": "true",
      "collection": short_name,
      "version": version_id,
      "discovery": "cmr",
      "mode": "cmr",
      "asset_name": "data",
      "asset_roles": ["data"]
    }
  ];
}

async function fetchCollections(title) {
  const resp = await fetch("https://cmr.maap-project.org/search/collections.json", {
    "headers": {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/111.0",
        "Accept": "application/vnd.nasa.cmr.umm_results+json; version=1.10",
        "Accept-Language": "en-US,en;q=0.5",
        "Content-Type": "application/x-www-form-urlencoded",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache"
    },
    "body": new URLSearchParams({ keyword: title.split(" ").join('* ') + '*' }).toString(),
    "method": "POST",
  });
  const result = await resp.json()
  // Dedup because for some reason CMR returns results multiple times
  const collections = result.feed.entry
    .map(parseCollection)
    .reduce(
      (a, [col, item]) => {
        return { 
          ... a, 
          [col.id]: [col, (col.id in a) ? [... a[col.id][1], item] : [item]]
        }
      },
      {}
    )
  await Promise.all(
    Object.entries(collections).map(
      async ([name, [col, items]]) => {
        await writeFile(`data/collections/${name}.json`, JSON.stringify(col, undefined, 2))
        await writeFile(`data/step_function_inputs/${name}.json`, JSON.stringify(items.length === 1 ? items[0] : items, undefined, 2))
      },
    )
  )
}
await fetchCollections(... argv.slice(2)) 
