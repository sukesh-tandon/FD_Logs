import logging
import azure.functions as func
from fastavro import reader as avro_reader
import io
import json
import pyodbc
import pytz
import re
from datetime import datetime
import os


# ---- TOKEN EXTRACTOR ----
TOKEN_REGEX = re.compile(r"/DUITAI/([A-Za-z0-9]+)", re.IGNORECASE)

def extract_token(url: str):
    if not url:
        return None
    match = TOKEN_REGEX.search(url)
    return match.group(1) if match else None


# ---- IST TIME CONVERTER ----
def convert_to_ist(utc_ts: str):
    try:
        dt = datetime.fromisoformat(utc_ts.replace("Z", "+00:00"))
        ist = pytz.timezone("Asia/Kolkata")
        return dt.astimezone(ist)
    except:
        return None


# ---- SAFE JSON FOR BYTES ----
def safe_json(obj):
    if isinstance(obj, (bytes, bytearray)):
        return obj.decode("utf-8", errors="ignore")
    return str(obj)


# ---- MAIN FUNCTION TRIGGER ----
def main(blob: func.InputStream):
    logging.info(f"Processing blob: {blob.name}")

    # Read AVRO bytes directly from blob trigger
    avro_data = blob.read()
    avro_stream = io.BytesIO(avro_data)

    # SQL Connection
    sql_conn_str = os.environ["SQL_CONN_STR"]
    try:
        cnxn = pyodbc.connect(sql_conn_str)
        cursor = cnxn.cursor()
        logging.info("SQL connection established")
    except Exception as e:
        logging.error(f"SQL connection error: {e}")
        return

    # Read AVRO content
    try:
        records = list(avro_reader(avro_stream))
    except Exception as e:
        logging.error(f"Failed to parse AVRO: {e}")
        return

    for rec in records:
        try:
            # Body → contains actual FD logs array
            body = json.loads(rec["Body"])
            logs = body.get("records", [])
        except Exception as e:
            logging.error(f"Invalid Body JSON: {e}")
            continue

        for log in logs:
            try:
                props = log.get("properties", {})

                # Extract time
                raw_utc = log.get("time")
                fd_time = convert_to_ist(raw_utc)

                # Extract useful fields
                request_uri = props.get("requestUri")
                origin_url = props.get("originUrl")

                token = extract_token(request_uri) or extract_token(origin_url)

                # Full safe JSON (raw)
                raw_json = json.dumps(log, default=safe_json)

                # INSERT INTO SQL
                cursor.execute("""
                    INSERT INTO fd_logs (
                        fd_time, client_ip, http_method, request_uri, user_agent,
                        http_status, cache_status, bytes_sent, activity_id,
                        route_name, backend_pool, edge_location, raw_json, token
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                fd_time,
                props.get("clientIp"),
                props.get("httpMethod"),
                request_uri,
                props.get("userAgent"),
                props.get("httpStatusCode"),
                props.get("cacheStatus"),
                props.get("responseBytes"),
                props.get("trackingReference"),
                props.get("routingRuleName"),
                props.get("originName"),
                props.get("pop"),
                raw_json,
                token)

            except Exception as e:
                logging.error(f"Error inserting record: {e}")

    # Commit & close
    try:
        cnxn.commit()
        cursor.close()
        cnxn.close()
        logging.info("Ingestion complete")
    except:
        pass
