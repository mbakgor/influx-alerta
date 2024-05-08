import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from datetime import datetime, timezone

from influxdb_client.client.write_api import SYNCHRONOUS

bucket = "bucket"
org = "org"
token = "token"
influxdb_url = "url"
alerta_url = "url"
headers = {'Authorization': 'Key key'}

client = InfluxDBClient(url=influxdb_url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

cache_file = 'alert_ids.txt'

def load_cached_ids():
    try:
        with open(cache_file, 'r') as file:
            return set(file.read().split())
    except FileNotFoundError:
        return set()

def save_cached_ids(processed_ids):
    with open(cache_file, 'w') as file:
        file.write(' '.join(processed_ids))

processed_ids = load_cached_ids()

def get_alerts():
    response = requests.get(alerta_url, headers=headers)
    print("Status Code:", response.status_code)
    print("Response Content:", response.text)

    try:
        data = response.json()
        return data.get('alerts', [])
    except requests.exceptions.JSONDecodeError:
        print("Failed to decode JSON from response")
        return []

def send_to_influxdb(alerts):
    points = []
    for alert in alerts:
        alert_id = alert.get('id', '')
        if alert.get('status') == 'open' and alert_id not in processed_ids:
            point = Point("alerts")\
                .field("customer", alert.get('customer', '')) \
                .tag("customer", alert.get('customer', '')) \
                .tag("event", alert.get('event', ''))\
                .tag("resource", alert.get('resource', ''))\
                .tag("id", alert_id)\
                .time(datetime.now(timezone.utc), WritePrecision.NS)
            points.append(point)
            processed_ids.add(alert_id)
    if points:
        write_api.write(bucket=bucket, record=points)
        print("Data written to InfluxDB")
    save_cached_ids(processed_ids)

def main():
    alerts = get_alerts()
    if alerts:
        send_to_influxdb(alerts)
    else:
        print("No alerts found or invalid response structure.")

if __name__ == "__main__":
    main()
