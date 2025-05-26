import paho.mqtt.client as mqtt
import json
from multiprocessing import Process
from uuid import uuid4
from datetime import datetime, timezone
import requests
import config as config
import time
import Context_Management_Service as cm

import threading

from threading import Event

# # MQTT and Context Broker configurations from config.py
FED_BROKER = config.FED_BROKER
FED_PORT = config.FED_PORT
CONTEXT_BROKER_URL = config.CONTEXT_BROKER_URL
FEDERATION_ID = config.FEDERATION_ID

def validate_forwarding(policy, federationID1, federationID2):
    # Access the sharing rules and permitted context types from the policy
    sharing_rules = policy.get("sharingRules", {}).get("value", [])
    permitted_context_types = policy.get("permittedContextTypes", {}).get("value", [])
    
    # Flags for the federation conditions
    federation1_can_forward = False
    federation2_canreceive = True

    # Loop through each federation in the sharing rules
    for federation_rule in sharing_rules:
        for federation_id, permissions in federation_rule.items():
            if federation_id == federationID1:
                # Check if federationID1 has canForward permission
                if str(permissions.get("canForward", "false")).lower() == "true":
                    federation1_can_forward = True

            elif federation_id == federationID2:
                # Mark federation2 as existing in the sharing rules
                if str(permissions.get("canReceive", "true")).lower() == "false":
                    federation2_canreceive = False

            elif str(federation_id).lower() == "public":
                # Handle 'public' federation permissions
                if str(permissions.get("canReceive", "false")).lower() == "true" and str(permissions.get("canForward", "false")).lower() == "true":
                    federation1_can_forward = federation1_can_forward or True

    # If federationID1 cannot forward, return False immediately
    if not federation1_can_forward:
        return [False]

    # Return True with permittedContextTypes if federation1 can forward and federation2 can receive
    if federation1_can_forward and federation2_canreceive:
        return [True, permitted_context_types]
    else:
        return [False]


def store_request(request):
    headers = {'Content-Type': 'application/json'}
    entity_id = request.get('id')  # Assuming the request contains an 'id' field

    if not entity_id:
        print("Request does not have an 'id' field. Cannot process.")
        return

    try:
        # Check if the entity already exists
        get_response = requests.get(f"{CONTEXT_BROKER_URL}/{entity_id}", headers=headers)
        if get_response.status_code == 200:  # Entity exists
            print(f"Entity {entity_id} already exists. Deleting it.")
            delete_response = requests.delete(f"{CONTEXT_BROKER_URL}/{entity_id}", headers=headers)
            delete_response.raise_for_status()
            print(f"Entity {entity_id} deleted successfully.")

        # Store the new request
        post_response = requests.post(CONTEXT_BROKER_URL, data=json.dumps(request), headers=headers)
        post_response.raise_for_status()
        print("Request stored successfully.")

    except requests.exceptions.HTTPError as http_err:
        if get_response.status_code == 404:  # Entity does not exist, proceed to store it
            print(f"Entity {entity_id} does not exist. Creating new entity.")
            try:
                post_response = requests.post(CONTEXT_BROKER_URL, data=json.dumps(request), headers=headers)
                post_response.raise_for_status()
                print("Request stored successfully.")
            except requests.exceptions.RequestException as post_err:
                print(f"Failed to store request: {post_err}")
        else:
            print(f"HTTP Error: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection Error: Unable to connect to {CONTEXT_BROKER_URL}. Details: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout Error: Request to {CONTEXT_BROKER_URL} timed out. Details: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Failed to process request: {req_err}")


def create_policy_topics_2_new_collaborators(Current_frederation_Id, new_collaborator_Id,
                                            mosquitto_address, port):
    userdata = {'policies': []}
    client = mqtt.Client(client_id=f"new_collab{uuid4()}", userdata=userdata)
    message_dict = {}  # Dictionary to store the topics and policies
    processed_topics = set()  # Track processed topics
    done = False  # Indicates completion of processing

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected successfully")
            # Subscribe to the topic
            client.subscribe("Federation/+/Policy/#")
        else:
            print(f"Connection failed with code {rc}")

    client.on_connect = on_connect

    def on_message(client, userdata, msg):
        nonlocal done, message_dict

        try:
            # Avoid re-processing the same topic
            if msg.topic in processed_topics:
                return
            processed_topics.add(msg.topic)

            # Decode the payload and convert it to JSON
            policies = json.loads(msg.payload.decode('utf-8'))
            if not isinstance(policies, list):
                policies = [policies]

            for policy in policies:
                # Validate the providerFederation attribute
                provider_federation = policy.get("providerFederation", {})
                provider_object = provider_federation.get("object")

                if provider_object not in [Current_frederation_Id, new_collaborator_Id]:
                    # Create the dictionary with topic as key and policy as value
                    allow_forwarding = validate_forwarding(
                        policy,
                        Current_frederation_Id.split(":")[-1],
                        new_collaborator_Id.split(":")[-1]
                    )

                    if allow_forwarding[0] and any(word in (allow_forwarding[1] or "") for word in ["policy", "policies"]):
                        message_dict[msg.topic] = policy

        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON payload: {e}")
            print(f"Raw payload: {msg.payload.decode('utf-8')}")
        except KeyError as e:
            print(f"Missing key in policy: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

        # Exit loop if no more messages are expected
        if len(processed_topics) >= len(message_dict):
            done = True

    client.on_message = on_message

    try:
        client.connect(mosquitto_address, port)
        client.loop_start()

        # Keep looping until `done` is True
        while not done:
            pass  # Let the MQTT client handle processing asynchronously
            # time.sleep(0.1)  # Minimal delay to allow incoming messages

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.loop_stop()
        client.disconnect()

    return message_dict  # Return the dictionary of topics and policies


def run_destination_client(destination_broker_addr, destination_port_num, receiver_Fed_ID, request):
    request_id=request['id']
    try:
        destination_client = mqtt.Client(client_id=f"destination_{uuid4()}")
        destination_client.connect(destination_broker_addr, destination_port_num, 60)
        destination_client.publish(f'urn:ngsi-ld:Federation:{receiver_Fed_ID}/Collaboration/requests/{request_id}', json.dumps(request),retain=True)
        print("Request sent:", json.dumps(request, indent=2))
        destination_client.disconnect()
    except Exception as e:
        print("Failed to send request:", e)
    Process(target=store_request,args=(request,)).start() 
    
def send_collaboration_request(destination_broker_addr, destination_port_num, receiver_Fed_ID, details, policy_ID):
    request_id = f"urn:ngsi-ld:CollaborationRequest:{uuid4()}"
    topics=list(create_policy_topics_2_new_collaborators(config.FEDERATION_ID,
                                            "urn:ngsi-ld:Federation:{receiver_Fed_ID}",
                                            config.FED_BROKER,config.FED_PORT).keys())
    if not topics:
        topics=[]
    
    request = {
        "id": request_id,
        "type": "CollaborationRequest",
        "sender": {"type": "Property", "value": FEDERATION_ID},
        "senderAddress": {"type": "Property", "value": f"{FED_BROKER}:{FED_PORT}"},
        "receiver": {"type": "Property", "value": f"urn:ngsi-ld:Federation:{receiver_Fed_ID}"},
        "requestDetails": {"type": "Property", "value": details},
        "timestamp": {"type": "Property", "value": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")},
        "status": {"type": "Property", "value": "Pending"},
        "policyID": {"type": "Property", "value": f"urn:ngsi-ld:ContextPolicy:{policy_ID}"},
        "policiesTopics":{"type": "Relationship", "object": topics}
    }
    
    
    print(json.dumps(request, indent=2))

    Process(target=run_destination_client, args=(destination_broker_addr, destination_port_num, receiver_Fed_ID, request)).start()

# if __name__ == "__main__":
#     send_collaboration_request(
#         destination_broker_addr="localhost",
#         destination_port_num=1861,
#         receiver_Fed_ID="Federation2",
#         details="start a collaboration test",
#         policy_ID="Policy1"
#     )


