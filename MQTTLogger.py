import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import logging
from requests.exceptions import ConnectionError
import os.path
import json
import signal
import sys

DB_SERVER = "192.168.0.3"
DB_PORT = 8086
DB_NAME = "home"
MQTT_SERVER = "127.0.0.1"
MQTT_PORT = 1883
BASE_SUBSCRIPTION = "home/#"
KNOWN_MEASUREMENTS = ['temperature', 'humidity']
JSON_FILENAME = "measurements_config.json"

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT server.")
    print("Subscribing", BASE_SUBSCRIPTION)

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(BASE_SUBSCRIPTION)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    topic = msg.topic.split('/')
    device = topic[1]
    measurement = topic[2]
    if measurement in KNOWN_MEASUREMENTS:
        conf = get_series_conf(device, measurement)
        new_point = [
        {
            "measurement": measurement,
            "tags": {
                "device": device,
            },
            "fields": {
                "value": float(msg.payload)
            }
        }
        ]
        print(new_point)
        db.write_points(new_point)
    else:
        print(measurement, "is not a recognised measurement type.")

def get_series_conf(device, measurement):
    if device not in config.keys():
        config[device] = {}
    if measurement not in config[device].keys():
        config[device][measurement] = {"filter_type": "none", "filter": 0}
        save_config_json(config)
    return config[device][measurement]

def load_config_json():
    data = {}
    if os.path.exists(JSON_FILENAME):
        with open(JSON_FILENAME) as f:
            data = json.load(f)
    return data

def save_config_json(data):
    with open(JSON_FILENAME, "w+") as f:
        json.dump(data, f, indent=4)

def sig_exit(signum, frame):
    print("Exiting.")
    client.disconnect()

signal.signal(signal.SIGINT, sig_exit)
signal.signal(signal.SIGTERM, sig_exit)
config = load_config_json()
db = InfluxDBClient(DB_SERVER, DB_PORT, database=DB_NAME)
try:
    db.ping()

    client = mqtt.Client()
    logger = logging.getLogger(__name__)
    client.enable_logger(logger)

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_SERVER, MQTT_PORT, 60)

    client.loop_forever()

except ConnectionError:
    print("Could not connect to DB at", DB_SERVER)
    sys.exit(1)