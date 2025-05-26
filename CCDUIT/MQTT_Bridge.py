import paho.mqtt.client as mqtt
import logging
import config
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4
import time

# Define a custom MQTTClient class for managing multiple brokers
class MQTTClient(mqtt.Client):
    def __init__(self, cname, **kwargs):
        # Initialize the parent class with the correct protocol
        super(MQTTClient, self).__init__(client_id=cname, **kwargs)
        self.cname = cname
        self.connected_flag = False
        self.bad_connection_flag = False
        self.broker = ""
        self.port = 1883
        self.keepalive = 60
        self.sub_topic = ""

# MQTT callbacks
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        client.connected_flag = True
        print(f"Connected to broker: {client.broker}")
        if client.sub_topic:
            options = mqtt.SubscribeOptions(qos=1)  # Customize QoS if needed
            client.subscribe(client.sub_topic, options=options)
    else:
        client.bad_connection_flag = True
        print(f"Connection failed with error code: {rc}")

def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print(f"Subscribed to topic: {client.sub_topic}")

def on_message(client, userdata, msg):
    topic = msg.topic
    message = msg.payload.decode('utf-8', errors='ignore').strip()
    # print(f"Message received on topic {topic} from {client.broker}: {message}")
    client.disconnect()
    message_routing(client, topic, message)

def message_routing(client, topic, message):
    # print(f"Forwarding message from {client.broker} to all other brokers.")

    def publish_message(broker_client):
        try:
            # Add properties to avoid feedback
            properties = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
            properties.UserProperty = [("PublisherID", config.FEDERATION_ID)]
            broker_client.publish(topic, message, qos=1, properties=properties, retain=True)
            print(f"Message forwarded to broker: {broker_client.broker}")
        except Exception as e:
            print(f"Failed to publish to broker {broker_client.broker}: {e}")
        finally:
            # Disconnect the client after publishing
            broker_client.loop_stop()
            broker_client.disconnect()
            print(f"Disconnected broker: {broker_client.broker}")

    with ThreadPoolExecutor() as executor:
        executor.map(publish_message, destination_clients)

# Dynamic setup of brokers
def setup_brokers(source_broker, source_port, dest_brokers, topics):
    global source_client, destination_clients

    def initialize_and_connect(cname, broker, port, sub_topic=""):
        try:
            client = MQTTClient(cname,protocol=mqtt.MQTTv5)
            client.broker = broker
            client.port = port
            client.sub_topic = sub_topic
            client.on_connect = on_connect
            client.on_subscribe = on_subscribe
            client.on_message = on_message

            client.connect(broker, port, client.keepalive==60)
            client.loop_start()
            print(f"Client '{cname}' connected to {broker}")
            return client
        except Exception as e:
            print(f"Failed to initialize/connect client '{cname}' to {broker}: {e}")
            return None

    client_configs = [{"cname": "bridge-c1", "broker": source_broker, "port": source_port, "sub_topic": topics[0]}]
    client_configs += [
        {"cname": f"bridge-c2-{i}", "broker": broker["host"], "port": broker["port"], "sub_topic": ""}
        for i, broker in enumerate(dest_brokers)
    ]

    with ThreadPoolExecutor() as executor:
        clients = list(executor.map(
            lambda config: initialize_and_connect(config["cname"], config["broker"], config["port"], config["sub_topic"]),
            client_configs
        ))

    clients = [client for client in clients if client is not None]

    source_client = clients[0]
    destination_clients = clients[1:]

    return clients

# # Source broker configuration
# SOURCE_BROKER = "localhost"  # e.g., "broker1.mqtt.com"
# SOURCE_PORT = 1860                       # Port of the source broker
# SOURCE_TOPIC = "topic/test"            # Topic to subscribe to on the source broker

# # Destination broker configuration
# DESTINATION_BROKERS = [
#     {"host": "localhost", "port": 1861},
#     {"host": "localhost", "port": 1862}
# ]

# # Setup MQTT bridge
# clients = setup_brokers(
#     source_broker=SOURCE_BROKER,
#     source_port=SOURCE_PORT,
#     dest_brokers=DESTINATION_BROKERS,
#     topics=[SOURCE_TOPIC]
# )
# try:
#     while True:
#         time.sleep(1)
# except KeyboardInterrupt:
#     print("Shutting down...")
#     for client in clients:
#         client.loop_stop()
#         client.disconnect()
