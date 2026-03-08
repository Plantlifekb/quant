import json, logging
logging.basicConfig(level=logging.DEBUG)
from quant.engine.tasks.ingestion import task_ingest_and_write
res = task_ingest_and_write()
print(json.dumps(res, indent=2))
