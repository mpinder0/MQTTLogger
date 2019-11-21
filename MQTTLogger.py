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
CONFIG_FILENAME = "data.json"
NEW_DEVICE_DATA = {
    "series": {},
    "timeout_seconds": 120,
    "online": False,
    "last_time": None
}
NEW_MEASUREMENT_DATA = {
    "filter_type": "none",
    "filter": 0.0,
    "last_value": None
}
path = os.path.dirname(__file__)
config_path = os.path.join(path, CONFIG_FILENAME)

logging.basicConfig()
logger = logging.getLogger("MQTTLogger")
logger.setLevel(logging.DEBUG)

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    global logger
    logger.info("Connected to MQTT server.")
    logger.info("Subscribing {}".format(BASE_SUBSCRIPTION))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(BASE_SUBSCRIPTION)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    global config
    topic = msg.topic.split('/')
    device = topic[1]
    measurement = topic[2]
    if measurement in KNOWN_MEASUREMENTS:
        create_series_conf(device, measurement)
        val = float(msg.payload)
        if filter_value(config, device, measurement, val):
            db_write_point(device, measurement, val)

        config[device]["last_time"] = time.time()
        config[device]["series"][measurement]["last_value"] = val
        if not config[device]["online"]:
            config[device]["online"] = True
            db_write_point(device, "status", True)
    else:
        logger.warning("{} is not a recognised measurement type.".format(measurement))

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
    logger.debug(new_point)
    db.write_points(new_point)

def filter_value(conf, device, series, value):
    series_conf = conf[device]["series"][series]
    last_value = series_conf["last_value"]
    if conf[device]["online"] and last_value is not None:
        if series_conf['filter_type'] == 'absolute':
            low = last_value - series_conf['filter']
            high = last_value + series_conf['filter']
            if low < value < high:
                return False
    # if anything else, return true
    return True

def create_series_conf(device, measurement):
    global config
    if device not in config.keys():
        config[device] = NEW_DEVICE_DATA
    if measurement not in config[device]["series"].keys():
        config[device]["series"][measurement] = NEW_MEASUREMENT_DATA
        save_config_json(config)

def load_config_json():
    data = {}
    if os.path.exists(config_path):
        with open(config_path) as f:
            data = json.load(f)
    return data

def save_config_json(data):
    with open(config_path, "w+") as f:
        json.dump(data, f, indent=4)

def sig_exit(signum, frame):
    global run
    global config
    logger.info("Exiting.")
    client.loop_stop()
    run = False
    for d in config.values():
        d["online"] = False
    save_config_json(config)

run = True
signal.signal(signal.SIGINT, sig_exit)
signal.signal(signal.SIGTERM, sig_exit)
config = load_config_json()
db = InfluxDBClient(DB_SERVER, DB_PORT, database=DB_NAME)
try:
    db.ping()

    client = mqtt.Client()
    client.enable_logger(logger)

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_SERVER, MQTT_PORT, 60)

    client.loop_start()

    while run:
        online_devices = {k:v for k,v in config.items() if v["online"]}
        for device_name, device in online_devices.items():
            if time.time() > device["last_time"] + device['timeout_seconds']:
                logger.info("device {} is offline.".format(device_name))
                db_write_point(device_name, "status", False)
                device["online"] = False
        time.sleep(1)

except ConnectionError:
    logger.info("Could not connect to DB at {}".format(DB_SERVER))
    sys.exit(1)