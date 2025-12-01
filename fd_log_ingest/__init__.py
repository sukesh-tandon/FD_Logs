import logging
import json
import azure.functions as func
from fastavro import reader as avro_reader
import pyodbc
import io
import re
import os

# SQL connection string from environment variable
SQL_CONN_STR = os.environ["SQL_CONN_STR"]

token_regex = re.compile(r"/([A-Za-z0-9]{4,12})(?:\?|/|$)")

def extract_token(uri: str):
    if not uri:
        return None
    m = token_regex.search(uri)
    return m.group(1) if m else None


def main(blob: func.InputStream):
    logging.info(f"Processing blob: {blob.name}")

    try:
        avro_bytes = blob.read()
        avro_stream = io.BytesIO(avro_bytes)
        records = list(avro_reader(avro_stream))
    except Exception as e:
        logging.error(f"Failed to parse AVRO: {str(e)}")
        return

    if not records:
        logging.info("No records in this file.")
        return

    try:
        conn = pyodbc.connect(SQL_CONN_STR)
        cursor = conn.cursor()
    except Exception as e:
        logging.error(f"SQL connection error: {str(e)}")
        return

    for rec in records:

        fd_time = rec.get("time")
        client_ip = rec.get("clientIp")
        http_method = rec.get("httpMethod")
        request_uri = rec.get("requestUri")
        user_agent = rec.get("userAgent")
        referrer = rec.get("referrer")
        http_status = rec.get("httpStatus")
        cache_status = rec.get("cacheStatus")
        bytes_sent = rec.get("sentBytes")
        activity_id = rec.get("activityId")
        route_name = rec.get("routeName")
        backend_pool = rec.get("backendPoolId")
        edge_location = rec.get("edgeLocationId")

        token = extract_token(request_uri)
        raw_json = json.dumps(rec)

        cursor.execute("""
            INSERT INTO dbo.FD_RawLogs (
                fd_time, client_ip, http_method, request_uri, user_agent, referrer,
                http_status, cache_status, bytes_sent, activity_id, route_name,
                backend_pool, edge_location, raw_json, token
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        fd_time, client_ip, http_method, request_uri, user_agent, referrer,
        http_status, cache_status, bytes_sent, activity_id, route_name,
        backend_pool, edge_location, raw_json, token)

    conn.commit()
    cursor.close()
    conn.close()

    logging.info(f"Ingested {len(records)} records.")
