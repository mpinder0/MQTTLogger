import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import logging
from requests.exceptions import ConnectionError

DB_SERVER = "127.0.0.1"
DB_PORT = 8086
DB_NAME = "home"
MQTT_SERVER = "127.0.0.1"
MQTT_PORT = 1883
BASE_SUBSCRIPTION = "home/#"
KNOWN_MEASUREMENTS = ['temperature', 'humidity']

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
    if topic[2] in KNOWN_MEASUREMENTS:
        json_body = [
        {
            "measurement": topic[2],
            "tags": {
                "device": topic[1],
            },
            "fields": {
                "value": float(msg.payload)
            }
        }
        ]
        print(json_body)
        db.write_points(json_body)
    else:
        print(topic[2], "is not a recognised measurement type.")

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
    print("Exiting.")