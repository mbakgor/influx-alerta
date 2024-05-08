# influx-alerta
This script gets data from Alerta API and write these data to Influxdb v2 without any duplicate.

There is too many attributes we get from Alerta.io API. This script tooks the attributes I need and writes to the bucket as a "Field" and "Tag". 

You need to change the attributes name in "send_to_influxdb(alerts)" function. and use the ".tag" and ".field" as how you want to store datas in bucket. 
