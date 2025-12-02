import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from fastavro import reader
import io
import pyodbc
import json
from datetime import datetime
import pytz
import re
import os


def extract_token(url: str):
    if not url:
        return None
    match = re.search(r"/DUITAI/([A-Za-z0-9]+)", url)
    return match.group(1) if match else None


def convert_to_ist(utc_time_str):
    try:
        dt_utc = datetime.fromisoformat(utc_time_str.replace("Z", "+00:00"))
        ist = pytz.timezone("Asia/Kolkata")
        return dt_utc.astimezone(ist)
    except:
        return None


def main(myblob: func.InputStream):
    logging.info(f"Processing blob: {myblob.name}")

    blob_conn_str = os.environ["FDLOGS_STORAGE_CONN"]
    sql_conn_str = os.environ["SQL_CONN_STR"]

    # Download AVRO blob
    blob_service = BlobServiceClient.from_connection_string(blob_conn_str)
    container_name = myblob.name.split("/")[0]

    blob_client = blob_service.get_blob_client(
        container=container_name,
        blob="/".join(myblob.name.split("/")[1:])
    )

    avro_bytes = blob_client.download_blob().readall()
    bytes_reader = io.BytesIO(avro_bytes)

    # Prepare SQL connection
    try:
        cnxn = pyodbc.connect(sql_conn_str)
        cursor = cnxn.cursor()
    except Exception as e:
        logging.error(f"SQL connection error: {e}")
        return

    # Process AVRO
    for rec in reader(bytes_reader):

        try:
            body = json.loads(rec["Body"])
            for log in body["records"]:
                props = log.get("properties", {})

                fd_time_utc = log.get("time")
                fd_time_ist = convert_to_ist(fd_time_utc)

                request_uri = props.get("requestUri")
                origin_url = props.get("originUrl")

                token = extract_token(request_uri) or extract_token(origin_url)

                sql = """
                INSERT INTO fd_logs
                (fd_time, client_ip, http_method, request_uri, user_agent,
                 http_status, cache_status, bytes_sent, activity_id,
                 route_name, backend_pool, edge_location, raw_json, token)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """

                cursor.execute(sql, (
                    fd_time_ist,
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
                    json.dumps(log),
                    token
                ))

            cnxn.commit()

        except Exception as e:
            logging.error(f"Error processing record: {e}")

    cursor.close()
    cnxn.close()
