# file: lambda_function.py
import os
import json
import uuid
import boto3
import base64
import datetime
import urllib.parse
import urllib.request
import urllib.error
import time

s3 = boto3.client("s3")

# --- ENV ---
BUCKET       = os.getenv("BUCKET_NAME")                       # ex: bucket-cityflow-paris-s3-raw
BASE_PREFIX  = os.getenv("BASE_PREFIX", "raw/api")            # ex: raw/api
ENABLE       = [s.strip() for s in os.getenv("ENABLE_SOURCES", "weather,traffic,bikes").split(",") if s.strip()]

# Weather (Visual Crossing)
WEATHER_BASE_URL = os.getenv("WEATHER_BASE_URL")              # ex: https://.../timeline/Paris?unitGroup=metric&contentType=json
WEATHER_API_KEY  = os.getenv("WEATHER_API_KEY")               # ex: abc123

# Traffic (IDFM *ou* Navitia)
TRAFFIC_MODE    = os.getenv("TRAFFIC_MODE", "idfm").lower()   # "idfm" | "navitia"
TRAFFIC_URL     = os.getenv("TRAFFIC_URL")                    # idfm: https://prim.../v2/navitia/coverage/fr-idf/disruptions
                                                             # navitia: https://api.navitia.io/v1/coverage/fr-idf/disruptions
TRAFFIC_API_KEY = os.getenv("TRAFFIC_API_KEY")                # idfm: header apikey ; navitia: token (Basic, pwd vide)

# Bikes (Vélib GBFS)
BIKES_URL       = os.getenv("BIKES_URL")                      # https://velib-metropole-opendata.smoove.pro/gbfs/en/station_status.json

# Réseau
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))     # seconds (connect+read)
HTTP_RETRIES    = int(os.getenv("HTTP_RETRIES", "1"))         # nb de retries en plus de la 1re tentative

DEFAULT_HEADERS = {
    "Accept": "application/json",
    "Accept-Encoding": "identity",
    "User-Agent": "cityflow-fetcher/1.1",
}

# --- Helpers ---
def _now_parts():
    now = datetime.datetime.utcnow()
    return now.strftime("%Y-%m-%d"), now.strftime("%H"), int(now.timestamp())

def _http_get(url: str, headers: dict | None = None, timeout: int = 10, basic_auth: tuple | None = None) -> bytes:
    hdrs = dict(DEFAULT_HEADERS)
    if headers:
        hdrs.update(headers)

    if basic_auth:
        user, pwd = basic_auth
        token = base64.b64encode(f"{user}:{pwd}".encode("utf-8")).decode("ascii")
        hdrs["Authorization"] = f"Basic {token}"

    # retries simples
    attempts = 1 + max(0, HTTP_RETRIES)
    last_err = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers=hdrs)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
            last_err = e
            if i < attempts - 1:
                time.sleep(0.8 * (i+1))
            else:
                raise
    # ne devrait pas arriver
    raise last_err if last_err else RuntimeError("unknown http error")

def _bytes_to_records(raw: bytes):
    """
    Ecrit en JSON Lines. dict -> 1 ligne ; list -> N lignes ; sinon -> {"_raw": "..."}.
    """
    try:
        data = json.loads(raw.decode("utf-8"))
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        return [{"_value": data}]
    except Exception:
        return [{"_raw": raw.decode("utf-8", errors="replace")}]

def _write_jsonl(bucket, key, records):
    payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
    if records:
        payload += "\n"
    s3.put_object(Bucket=bucket, Key=key, Body=payload.encode("utf-8"), ContentType="application/json")

def _save_source(name: str, url: str, headers: dict | None = None, basic_auth: tuple | None = None):
    date_str, _hour_str, epoch = _now_parts()
    key = f"{BASE_PREFIX}/{name}/{date_str}/stream-data-{epoch}-{uuid.uuid4().hex}.json"
    raw = _http_get(url, headers=headers, timeout=REQUEST_TIMEOUT, basic_auth=basic_auth)
    records = _bytes_to_records(raw)
    _write_jsonl(BUCKET, key, records)
    return {"source": name, "ok": True, "count": len(records), "key": key}

# --- Handler ---
def lambda_handler(event, context):
    if not BUCKET:
        return {"statusCode": 500, "body": json.dumps({"error": "BUCKET_NAME missing"})}

    results = []

    for src in ENABLE:
        s = src.lower().strip()
        try:
            if s == "weather":
                if not WEATHER_BASE_URL or not WEATHER_API_KEY:
                    results.append({"source": s, "ok": False, "error": "WEATHER_BASE_URL or WEATHER_API_KEY missing"})
                    continue
                # Ajoute la clé en query (si absente)
                sep = "&" if "?" in WEATHER_BASE_URL else "?"
                url = f"{WEATHER_BASE_URL}{sep}key={urllib.parse.quote(WEATHER_API_KEY)}"
                results.append(_save_source("weather", url))

            elif s == "traffic":
                if not TRAFFIC_URL or not TRAFFIC_API_KEY:
                    results.append({"source": s, "ok": False, "error": "TRAFFIC_URL or TRAFFIC_API_KEY missing"})
                    continue

                if TRAFFIC_MODE == "idfm":
                    # IDFM Marketplace (proxy Navitia) -> header apikey OBLIGATOIRE
                    headers = {"apikey": TRAFFIC_API_KEY}
                    results.append(_save_source("traffic", TRAFFIC_URL, headers=headers))
                elif TRAFFIC_MODE == "navitia":
                    # Navitia direct -> Basic Auth (token en user, mdp vide)
                    results.append(_save_source("traffic", TRAFFIC_URL, basic_auth=(TRAFFIC_API_KEY, "")))
                else:
                    results.append({"source": s, "ok": False, "error": f"unknown TRAFFIC_MODE={TRAFFIC_MODE}"})

            elif s == "bikes":
                if not BIKES_URL:
                    results.append({"source": s, "ok": False, "error": "BIKES_URL missing"})
                    continue
                results.append(_save_source("bikes", BIKES_URL))

            else:
                results.append({"source": s, "ok": False, "error": "unknown source"})

        except urllib.error.HTTPError as e:
            detail = ""
            try:
                detail = e.read().decode("utf-8", errors="ignore")[:300]
            except Exception:
                pass
            results.append({"source": s, "ok": False, "error": f"HTTP {e.code}: {e.reason}", "detail": detail})
        except urllib.error.URLError as e:
            results.append({"source": s, "ok": False, "error": f"URL error: {getattr(e, 'reason', str(e))}"})
        except Exception as e:
            results.append({"source": s, "ok": False, "error": str(e)})

    status = 200 if any(r.get("ok") for r in results) else 500
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(results, ensure_ascii=False),
    }