"""Emit only Connector credentials, for the host-side restricted-file exporter."""
import json
import os
from pathlib import Path
from credentials import connector_credentials, valid_connector_key

values = connector_credentials(json.loads(Path('/run/spark-secrets/security.json').read_text()), os.environ)
if not all(valid_connector_key(value) for value in values.values()):
    raise SystemExit('Connector credentials unavailable or invalid; check bootstrap and explicit key overrides.')
print(json.dumps(values))
