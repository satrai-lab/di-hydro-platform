import json
from datetime import datetime, timezone
import json
import paho.mqtt.client as mqtt
import requests
import config
from multiprocessing import Process
import time

CONTEXT_BROKER_URL = config.CONTEXT_BROKER_URL
FED_BROKER = config.FED_BROKER
FED_PORT = config.FED_PORT
FEDERATION_ID = config.FEDERATION_ID
# ------------------------------------------------------------------------------

def store_policy(policy):
    # print(json.dumps(policy))
    if policy is None:
        print("Policy is None, skipping storage.")
        return
    if isinstance(policy,str):
        policy=json.loads(policy)
        # print(policy)
    headers = {'Content-Type': 'application/json'}
    policy_id = policy.get('id', None)
    if policy_id is None:
        print("Policy ID is None, cannot store policy.")
        return

    print(f"Processing policy with ID: {policy_id}")

    # Extract the lastModified timestamp from the incoming policy
    incoming_last_modified_str = policy.get('modificationPolicy', {}).get('value', {}).get('lastModified', None)
    if incoming_last_modified_str is None:
        print("Incoming policy does not have a lastModified timestamp.")
        return

    # Convert incoming lastModified to UTC datetime
    try:
        incoming_last_modified = datetime.strptime(incoming_last_modified_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        print(f"Invalid lastModified format in incoming policy: {incoming_last_modified_str}")
        return

    try:
        # Attempt to retrieve the existing policy from the Context Broker
        response = requests.get(f"{CONTEXT_BROKER_URL}/{policy_id}")
        if response.status_code == 200:
            existing_policy = response.json()
            existing_last_modified_str = existing_policy.get('modificationPolicy', {}).get('value', {}).get('lastModified', None)

            if existing_last_modified_str:
                # Convert existing lastModified to UTC datetime
                try:
                    existing_last_modified = datetime.strptime(existing_last_modified_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                except ValueError:
                    print(f"Invalid lastModified format in existing policy: {existing_last_modified_str}")
                    return

                # Compare the timestamps
                if incoming_last_modified > existing_last_modified:
                    print(f"Incoming policy is newer. Updating policy {policy_id}.")
                    # Delete the existing policy
                    delete_response = requests.delete(f"{CONTEXT_BROKER_URL}/{policy_id}")
                    delete_response.raise_for_status()
                    print(f"Policy {policy_id} deleted successfully.")
                else:
                    print(f"Existing policy {policy_id} is up-to-date. No changes made.")
                    return

        # Store the incoming policy
        response = requests.post(CONTEXT_BROKER_URL, json=policy, headers=headers)
        response.raise_for_status()
        print(f"Policy {policy_id} stored successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to process policy {policy_id}: {e}")



def delete_policy(policy_id):
    """
    Checks if a policy exists in the Orion-LD broker and deletes it if it exists.

    :param broker_url: The base URL of the Orion-LD broker (str).
    :param policy_id: The ID of the policy to check and delete (str).
    """
    headers = {
        "Accept": "application/json",
    }

    # Construct the URL to fetch the entity
    entity_url = f"{CONTEXT_BROKER_URL}/{policy_id}"

    try:
        # Check if the policy exists
        response = requests.get(entity_url, headers=headers)
        
        if response.status_code == 200:
            print(f"Policy {policy_id} exists. Proceeding to delete it...")
            
            # Delete the policy
            delete_response = requests.delete(entity_url, headers=headers)
            if delete_response.status_code == 204:
                print(f"Policy {policy_id} successfully deleted from the Orion-LD broker.")
            else:
                print(f"Failed to delete policy {policy_id}. Status code: {delete_response.status_code}")
        elif response.status_code == 404:
            print(f"Policy {policy_id} does not exist in the Orion-LD broker.")
        else:
            print(f"Error while checking policy {policy_id}. Status code: {response.status_code}")
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


def publish_policy(policy_entity,topic, mosquitto_address,port=1883):
    """
    Publishes a policy entity to the specified Mosquitto MQTT broker topic.

    Args:
        policy_entity (dict): The policy entity as a Python dictionary.
        mosquitto_address (str): The address of the Mosquitto broker (e.g., "localhost" or "192.168.1.100").
        topic (str, optional): The MQTT topic to publish to. Defaults to "fred/policy".
    """
    
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    # Create MQTT Client
    client = mqtt.Client()
    client.on_connect = on_connect

    # Connect to Mosquitto Broker
    try:
        client.connect(mosquitto_address,port)
    except Exception as e:
        print(f"Error connecting to MQTT broker: {e}")
        return

    # Start MQTT client loop in the background
    client.loop_start()
   
    # Convert entity to JSON string
    payload = json.dumps(policy_entity)

    # Publish message with QoS 2 for persistence
    result = client.publish(topic, payload, qos=2,retain=True)

    # Check if message was sent successfully
    if result[0] == 0:
        print(f"Policy sent to topic {topic}")
    else:
        print(f"Failed to send policy to topic {topic}")

    # Disconnect after publishing
    client.loop_stop()
    client.disconnect()

def register_start_times(start_time, file_name="../Results/start_times_Fed1.txt"):
    
    try:
        # Open the file in append mode, creating it if it doesn't exist
        with open(file_name, "a") as f:
            f.write(f"{start_time}\n")
    except Exception as e:
        print(f"An error occurred while appending to file: {e}")
    
    
def add_extra_slashes(url):
    return url.replace("/", "//")

def create_publish_policy(policy_ID,name,description,providerFederation_ID,permittedContextTypes,
                            sharingRules,modifiedBy,Geographic_Restrictions,mosquitto_address=FED_BROKER,mosquitto_Port=FED_PORT):
    # start_time=time.time_ns()
    modified_url = add_extra_slashes(CONTEXT_BROKER_URL)
    # Process(target=register_start_times, args=(start_time,)).start()
    policy_Entity = {
            "id": f"urn:ngsi-ld:ContextPolicy:{policy_ID}",
            "type": "ContextPolicy",
            "name": {
              "type": "Property",
              "value": f"{name}"
            },
            "description": {
              "type": "Property",
              "value": f"{description}"
            },
            "providerFederation": {
              "type": "Relationship",
              "object": f"urn:ngsi-ld:Federation:{providerFederation_ID}"
            },
            "permittedContextTypes": {
              "type": "Property",
              "value": permittedContextTypes
            },
            "ContextBrokerURL": {
              "type": "Property",
              "value": f"{modified_url}"
            },
            "sharingRules": {
              "type": "Property",
              "value": sharingRules
            },
            "modificationPolicy": {
              "type": "Property",
              "value": {
                "lastModified": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "modifiedBy": modifiedBy
              }
            },
            "Geographic_Restrictions": {
              "type": "Property",
                "value": Geographic_Restrictions
            }
}
    Process(target=store_policy,args=(policy_Entity,)).start()
    # store_policy(policy_Entity)
    # file_path=f"{policy_ID}.jsonld"
    # # Write policy to file with proper JSON-LD formatting
    # with open(file_path, "w") as f:
    #     json.dump(policy_Entity, f, indent=2)  # Use indent for readability
    publish_policy(policy_Entity,f"Federation/urn:ngsi-ld:Federation:{providerFederation_ID}/Policy/urn:ngsi-ld:ContextPolicy:{policy_ID}",
                  mosquitto_address,mosquitto_Port)
        

def on_message(client, userdata, msg):
    """
    Callback function triggered when a message is received on the subscribed topic.
    """
    try:
        policy = json.loads(msg.payload.decode())
        # print("Received Policy:")
        # print(json.dumps(policy, indent=4))  # Pretty-print the JSON
        userdata['policy'] = policy  # Store the policy in the userdata dictionary
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")

def subscribe_retrieve_policy(topic, mosquitto_address, port=FED_PORT, timeout=None):
    """
    Retrieves policy data from the specified MQTT topic.

    Args:
        topic (str): The MQTT topic to subscribe to.
        mosquitto_address (str): Address of the Mosquitto broker.
        port (int, optional): Port number of the Mosquitto broker. Defaults to 1883.
        timeout (int, optional): Time in seconds to wait for a policy message. 
                                If None (default), it waits indefinitely.

    Returns:
        dict or None: The retrieved policy as a dictionary, or None if an error occurs 
                      or the timeout is reached.
    """
    userdata = {'policy': None}  # Dictionary to store the retrieved policy
    client = mqtt.Client(userdata=userdata)
    client.on_message = on_message

    # Set up last will and testament for graceful disconnection
    client.will_set(topic, payload="Client Disconnected", qos=2, retain=True)

    def on_disconnect(client, userdata, rc):
        """Callback for when the client disconnects."""
        if rc == 0:
            client.loop_stop()  # Stop the loop gracefully

    client.on_disconnect = on_disconnect  # Set the disconnect callback

    try:
        client.connect(mosquitto_address, port)
        client.subscribe(topic)
        
        # Start MQTT client loop
        start_time = datetime.now()  # Track start time for timeout handling
        while userdata['policy'] is None:  # Wait for a policy to be received
            client.loop(timeout=1.0)  # Non-blocking loop with 1 second timeout

            if timeout is not None:
                elapsed_time = (datetime.now() - start_time).total_seconds()
                if elapsed_time > timeout:
                    print("Timeout reached. No policy received.")
                    break  # Exit the loop after timeout

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.disconnect()  # Ensure disconnection even if an error occurs

    return userdata['policy']  # Return the retrieved policy or None


def remove_topic_from_broker(broker_address, broker_port, federation_id, policy_id=None):
    """
    Removes a specific topic by clearing retained messages on the broker.
    Disconnects immediately after clearing the topic.

    :param broker_address: The address of the MQTT broker (str)
    :param broker_port: The port of the MQTT broker (int)
    :param federation_id: The federation ID (str)
    :param policy_id: The policy ID (str or None). If None, clears all policies under the federation.
    """
    # Define the topic
    if policy_id is None:
        print("Wildcard deletion of topics isn't supported; individual topics must be cleared.")
        return
    else:
        topic = f"Federation/{federation_id}/Policy/{policy_id}"
    
    # Define the MQTT client
    client = mqtt.Client()

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print(f"Connected to broker at {broker_address}:{broker_port}")
            # Publish an empty retained message to clear the topic
            client.publish(topic, payload=None, qos=1, retain=True)
            print(f"Cleared retained message for topic: {topic}")
            Process(target=delete_policy,args=(policy_id,)).start()
            client.disconnect()  # Disconnect immediately
        else:
            print(f"Failed to connect to broker. Return code: {rc}")

    def on_disconnect(client, userdata, rc):
        print("Disconnected from broker.")

    # Attach callback functions
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    try:
        # Connect to the broker
        client.connect(broker_address, broker_port, 60)
        client.loop_forever()  # Start loop to handle callbacks
    except Exception as e:
        print(f"An error occurred: {e}")

# import time
# #script for testing purposes:
# def main():
#     for i in range(1, 100):
#         create_publish_policy(
#             "Policy1",
#             "policy1 test",
#             "just testing 100 times",
#             "Federation1",
#             ["community", "federation", "policies", "functions"],
#             [
#                 {"Federation2": {"canReceive": "true", "canForward": "true"}},
#                 {"public": {"canReceive": "true", "canForward": "true"}},
#             ],
#             "Niemat",
#             []
#         )
#         time.sleep(2)

# if __name__ == "__main__":
#     main()


