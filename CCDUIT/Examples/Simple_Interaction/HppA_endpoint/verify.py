import paho.mqtt.client as mqtt

# MQTT Configuration
mqtt_broker = 'localhost'  # Update with your MQTT broker address
mqtt_port = 1888  # Update with your MQTT port
mqtt_topic='community2/Temperature'  # Update with your MQTT topic
# Callback function to handle messages
def on_message(client, userdata, message):
    print(f"Message received on topic {message.topic}:")
    try:
        # Decode the payload
        turtle_data = message.payload.decode('utf-8', errors='ignore').strip()
        print("Received Turtle Data:")
        print(turtle_data)  # Print the Turtle data to the console for verification
    except UnicodeDecodeError as e:
        print(f"Decoding error: {e}")

# Callback function for connection
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker successfully.")
        client.subscribe(mqtt_topic)  # Subscribe to the topic
    else:
        print(f"Connection failed with code {rc}")

# Set up MQTT client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Connect to the MQTT broker and start listening
client.connect(mqtt_broker, mqtt_port, 60)
client.loop_forever()  # Keep the client running to listen for incoming messages
