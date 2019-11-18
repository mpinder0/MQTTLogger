import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import logging
from requests.exceptions import ConnectionError
import os.path
import json
import signal
import sys
import time

DB_SERVER = "192.168.0.3"
DB_PORT = 8086
DB_NAME = "home"
MQTT_SERVER = "127.0.0.1"
MQTT_PORT = 1883
BASE_SUBSCRIPTION = "home/#"
KNOWN_MEASUREMENTS = ['temperature', 'humidity']
CONFIG_FILENAME = "measurements_config.json"
NEW_CONFIG = {
    "filter_type": "none",
    "filter": 0.0,
    "retained_days": 365,
    "timeout_seconds": 120
}
last_received = {}

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
        val = float(msg.payload)
        if filter_value(conf, device, measurement, val):
            db_write_point(device, measurement, val)
        else:
            print('value', val, 'did not meet filter criteria -', conf['filter_type'], ':', conf['filter'])

        set_last_received(device, measurement, val)
    else:
        print(measurement, "is not a recognised measurement type.")

def db_write_point(device, measurement, value):
    new_point = [
        {
            "measurement": measurement,
            "tags": {
                "device": device,
            },
            "fields": {
                "value": value
            }
        }
    ]
    print(new_point)
    db.write_points(new_point)

def set_last_received(device, measurement, value):
    global last_received
    if device not in last_received.keys():
        last_received[device] = {}
    last_received[device][measurement] = (time.time(), value)

def filter_value(config, device, measurement, value):
    global last_received
    try:
        last_value = last_received[device][measurement][1]
    except:
        return True

    if last_value == 'offline':
        return True

    if config['filter_type'] == 'absolute':
        low = last_value - config['filter']
        high = last_value + config['filter']
        if low < value < high:
            return False
        else:
            return True
    else:
        return True

def get_series_conf(device, measurement):
    global config
    if device not in config.keys():
        config[device] = {}
    if measurement not in config[device].keys():
        config[device][measurement] = NEW_CONFIG
        save_config_json(config)
    return config[device][measurement]

def load_config_json():
    data = {}
    if os.path.exists(CONFIG_FILENAME):
        with open(CONFIG_FILENAME) as f:
            data = json.load(f)
    return data

def save_config_json(data):
    with open(CONFIG_FILENAME, "w+") as f:
        json.dump(data, f, indent=4)

def sig_exit(signum, frame):
    global run
    print("Exiting.")
    client.loop_stop()
    run = False

run = True
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

    client.loop_start()

    while run:
        for device_k, device_v in last_received.items():
            for measurement_k, measurement_v in device_v.items():
                if measurement_v[1] is not 'offline':
                    print(config[device_k][measurement_k])
                    print(measurement_v[0])
                    if time.time() > measurement_v[0] + config[device_k][measurement_k]['timeout_seconds']:
                        db_write_point(device_k, measurement_k, False)
                        set_last_received(device_k, measurement_k,"offline")
        print('.')
        time.sleep(1)

except ConnectionError:
    print("Could not connect to DB at", DB_SERVER)
    sys.exit(1)