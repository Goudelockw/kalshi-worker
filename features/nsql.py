"""Tiny Neon client over the HTTPS SQL endpoint (works where port 5432 is blocked, e.g. scheduled
Claude sessions). Connection string from $DATABASE_URL, or a `.dburl` file next to this script."""
import json, os, time, urllib.request, urllib.error
from urllib.parse import urlparse
_U = os.environ.get("DATABASE_URL") or open(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".dburl")).read().strip()
_H = f"https://{urlparse(_U).hostname}/sql"

def q(sql, params=None, tries=3):
    body = json.dumps({"query": sql, "params": params or []}).encode()
    for i in range(tries):
        try:
            req = urllib.request.Request(_H, data=body, headers={"Neon-Connection-String": _U, "Content-Type": "application/json"})
            return json.loads(urllib.request.urlopen(req, timeout=300).read()).get("rows", [])
        except urllib.error.HTTPError as e:
            msg = e.read().decode()[:800]
            if i == tries - 1 or e.code == 400: raise RuntimeError(msg)
        except Exception:
            if i == tries - 1: raise
        time.sleep(2 * (i + 1))

def bulk(sql_with_json_param, rows, chunk=2000):
    """Run an INSERT ... FROM json_to_recordset($1::json) in chunks."""
    for i in range(0, len(rows), chunk):
        q(sql_with_json_param, [json.dumps(rows[i:i + chunk], default=str)])
