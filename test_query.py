import json
from pycarol import Carol, Query

carol = Carol(dotenv_path=".env")

query = {
    "mustList": [
        {
            "mdmFilterType": "TYPE_FILTER",
            "mdmValue": "requestsviewGolden"
        }
    ]
}
query_handler = Query(carol)
query_handler.query(json.dumps(query))
query_handler.go()
results = query_handler.results

