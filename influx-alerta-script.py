import requests
import logging
from influxdb_client import InfluxDBClient, Point, WritePrecision
from datetime import datetime
import pytz
import sqlite3

from influxdb_client.client.write_api import SYNCHRONOUS
from dateutil import parser

bucket = "YourBucket"
org = "YourOrg"
token = "YourTokenHere"
influxdb_url = "influx-url"
alerta_url = "alerta.io-url"
headers = {'Authorization': 'Key YourAPIKey'}

client = InfluxDBClient(url=influxdb_url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

db_path = '/path/to/db'
logging.basicConfig(filename='/root/logfile.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def connect_db():
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def create_table():
    with connect_db() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alertID TEXT NOT NULL UNIQUE,
                lastReceiveID TEXT,
                receiveTime TEXT,
                lastReceiveTime TEXT,
                customer TEXT,
                event TEXT,
                resource TEXT,
                duplicateCount INTEGER DEFAULT 0,
                isPushed BOOLEAN DEFAULT FALSE
            );
        ''')
        conn.commit()


def get_alerts():
    try:
        response = requests.get(alerta_url, headers=headers)
        logging.debug(f"Status Code: {response.status_code}")
        logging.debug(f"Response Content: {response.text}")
        response.raise_for_status()

        data = response.json()
        open_alerts = [alert for alert in data.get('alerts', []) if alert.get('status') == 'open']

        return open_alerts

    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        return []


def update_db_with_alerts(alerts):
    with connect_db() as conn:
        for alert in alerts:
            alert_id = alert.get('id', '')
            last_receive_id = alert.get('lastReceiveId', '')
            receive_time = alert.get('createTime', '')
            last_receive_time = alert.get('lastReceiveTime', '')
            cursor = conn.execute('SELECT * FROM alerts WHERE alertID=?', (alert_id,))
            existing_alert = cursor.fetchone()
            if existing_alert:
                conn.execute('''
                            UPDATE alerts SET
                                lastReceiveID=?,
                                lastReceiveTime=?,
                                duplicateCount=duplicateCount+1,
                                isPushed=0
                            WHERE alertID=?;
                        ''', (last_receive_id, last_receive_time, alert_id))
            else:
                conn.execute('''
                            INSERT INTO alerts (alertID, receiveTime, lastReceiveTime ,customer, event, resource, duplicateCount)
                            VALUES (?, ?, ?, ?, ?, ?, ?);
                        ''', (
                    alert_id, receive_time, receive_time, alert.get('customer', ''), alert.get('event', ''),
                    alert.get('resource', ''), '1'))
        conn.commit()


def send_to_influxdb():
    with connect_db() as conn:
        cursor = conn.execute('SELECT * FROM alerts WHERE isPushed=0')
        points = []
        for row in cursor:
            point = Point("alerts") \
                .field("customer", row['customer']) \
                .tag("customer", row['customer']) \
                .tag("event", row['event']) \
                .tag("resource", row['resource']) \
                .tag("create_time", datetime.fromisoformat(row['receiveTime'].rstrip('Z')).replace(tzinfo=pytz.utc).astimezone(pytz.timezone('Etc/GMT-3')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]) \
                .time(int(parser.parse(row['lastReceiveTime']).timestamp() * 1e9), WritePrecision.NS)
            points.append(point)
        if points:
            write_api.write(bucket=bucket, record=points)
            conn.execute('UPDATE alerts SET isPushed=1 WHERE isPushed=0')
            conn.commit()
            logging.info("Data written to InfluxDB")


def main():
    create_table()
    alerts = get_alerts()
    if alerts:
        update_db_with_alerts(alerts)
        send_to_influxdb()
    else:
        logging.info("No alerts found or invalid response structure.")


if __name__ == "__main__":
    main()
