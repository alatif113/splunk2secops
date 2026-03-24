import argparse
import json
import os
import requests
import time

from secops import SecOpsClient

def get_splunk_logs(base_url, token, index, sourcetype, earliest, latest, timeout):
    url = base_url.rstrip("/") + "/services/search/jobs/export"
    headers = {"Authorization": f"Splunk {token}"}

    search = f"search index={index} sourcetype={sourcetype}"

    resp = requests.post(
        url,
        headers=headers,
        data={
            "search": search,
            "output_mode": "json",       # newline-delimited JSON
            "earliest_time": earliest,
            "latest_time": latest,
            "count": 0,                  # all results
        },
        stream=True,
        timeout=timeout,
        verify=False,
    )
    resp.raise_for_status()

    for line in resp.iter_lines(decode_unicode=True):
        if not line:
            continue
        obj = json.loads(line)
        result = obj.get("result")
        if result and "_raw" in result:
            yield result["_raw"]

def chunk(items, batch_size):
    buf = []
    for x in items:
        buf.append(x)
        if len(buf) >= batch_size:
            yield buf
            buf = []
    if buf:
        yield buf

def ingest_to_secops(chronicle_client, log_type, batch):
    chronicle_client.ingest_log(log_type=log_type, log_message=batch)

def main():
    p = argparse.ArgumentParser()
    #Splunk
    p.add_argument("--base-url", default=os.getenv("SPLUNK_BASE_URL"), required=True)
    p.add_argument("--token", default=os.getenv("SPLUNK_TOKEN"), required=True)
    p.add_argument("--index", required=True)
    p.add_argument("--sourcetype", required=True)
    p.add_argument("--earliest", required=True)  # e.g. -24h@h
    p.add_argument("--latest", required=True)    # e.g. now

    # Google SecOps / Chronicle (wrapper)
    p.add_argument("--customer-id", required=True)
    p.add_argument("--project-id", required=True)
    p.add_argument("--region", default="us")     # e.g. us, europe, asia
    p.add_argument("--log-type", required=True)  # parser type in SecOps
    p.add_argument("--credentials-path", required=True)  # path to service account credentials

    # Optional
    p.add_argument("--splunk-timeout", type=int, default=500)
    p.add_argument("--batch-size", type=int, default=200)
    p.add_argument("--sleep-seconds", type=float, default=0.0)

    args = p.parse_args()

    client = SecOpsClient(service_account_path=args.credentials_path)
    chronicle = client.chronicle(customer_id=args.customer_id, project_id=args.project_id, region=args.region)

    raw = get_splunk_logs(args.base_url, args.token, args.index, args.sourcetype, args.earliest, args.latest, args.timeout)
    sent = 0
    for batch in chunk(raw, args.batch_size):
        ingest_to_secops(chronicle, args.log_type, batch)
        sent += len(batch)
        if args.sleep_seconds:
            time.sleep(args.sleep_seconds)

    print(f"Sent {sent} _raw events for index={args.index} sourcetype={args.sourcetype}.")
    
if __name__ == "__main__":
    main()
