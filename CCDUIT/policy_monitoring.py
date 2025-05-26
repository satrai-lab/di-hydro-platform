import paho.mqtt.client as mqtt
import json
from multiprocessing import Process
from uuid import uuid4
from datetime import datetime, timezone
import requests
import config as config
import time
import MQTT_Bridge as bridge
import threading
from threading import Event
import time
# MQTT and Context Broker configurations from config.py
FED_BROKER = config.FED_BROKER
FED_PORT = config.FED_PORT
CONTEXT_BROKER_URL = config.CONTEXT_BROKER_URL
FEDERATION_ID = config.FEDERATION_ID


def register_start_times(start_time, file_name="../Results/End_times_Fed1.txt"):
    try:
        # Open the file in append mode, creating it if it doesn't exist
        with open(file_name, "a") as f:
            f.write(f"{start_time}\n")
    except Exception as e:
        print(f"An error occurred while appending to file: {e}")
    

def Policy_Federation_Mapping_more(federation_id,policy_id):
    """
    Fetch all federations in collaboration with the given federation.
    """
    federations_in_collab_with = {}
    try:
        url = (
            f"{CONTEXT_BROKER_URL}?type=CollaborationRequest"
            f"&q=status==active&q=sender==%22{federation_id}%22&q=policyID==%22{policy_id}%22"
            f"&attrs=receiver&options=keyValues"
        )
        response = requests.get(url, headers={"Content-Type": "application/json"})
        response.raise_for_status()
        responses = response.json()

        if not isinstance(responses, list):
            responses = [responses]

        for response in responses:
            federation_with_collab_id = response.get("receiver")

            if federation_with_collab_id:
                try:
                    url_get = (
                        f"{CONTEXT_BROKER_URL}?type=CollaborationResponse"
                        f"&q=responseStatus==ok&q=sender==%22{federation_with_collab_id}%22"
                        f"&attrs=senderAddress&options=keyValues"
                    )
                    response_get = requests.get(url_get, headers={"Content-Type": "application/json"})
                    response_get.raise_for_status()
                    sender_data = response_get.json()

                    if isinstance(sender_data, list):
                        sender_data = sender_data[0]

                    sender_address = sender_data.get("senderAddress")
                    if sender_address:
                        federations_in_collab_with[federation_with_collab_id] = sender_address
                except requests.exceptions.RequestException as e:
                    # print(f"Error fetching request details: {e}")
                    continue
    except requests.exceptions.RequestException as e:
        print(f"Error fetching federation collaborations: {e}")
        return None

    return federations_in_collab_with


def Policy_Federation_Mapping(federation_id,policy_id):
    """
    Fetch all federations in collaboration with the given federation.
    """
    federations_in_collab_with = {}
    try:
        url = (
            f"{CONTEXT_BROKER_URL}?type=CollaborationResponse"
            f"&q=responseStatus==ok&q=sender==%22{federation_id}%22&q=policyID==%22{policy_id}%22"
            f"&attrs=receiver,responseTo&options=keyValues"
        )
        response = requests.get(url, headers={"Content-Type": "application/json"})
        response.raise_for_status()
        responses = response.json()

        if not isinstance(responses, list):
            responses = [responses]

        for response in responses:
            federation_with_collab_id = response.get("receiver")
            request_id = response.get("responseTo")

            if federation_with_collab_id and request_id:
                try:
                    url_get = f"{CONTEXT_BROKER_URL}/{request_id}?attrs=senderAddress&options=keyValues"
                    response_get = requests.get(url_get, headers={"Content-Type": "application/json"})
                    response_get.raise_for_status()
                    sender_data = response_get.json()

                    if isinstance(sender_data, list):
                        sender_data = sender_data[0]

                    sender_address = sender_data.get("senderAddress")
                    if sender_address:
                        federations_in_collab_with[federation_with_collab_id] = sender_address
                except requests.exceptions.RequestException as e:
                    # print(f"Error fetching request details: {e}")
                    continue
    except requests.exceptions.RequestException as e:
        print(f"Error fetching federation collaborations: {e}")
        return None

    # Fetch additional responses where the given federation is the receiver
    more_federations_mapping = Policy_Federation_Mapping_more(federation_id,policy_id)
    if more_federations_mapping:
        federations_in_collab_with.update(more_federations_mapping)
        

    return federations_in_collab_with


def fetch_response_by_federation_receiver(federation_id):
    """
    Fetch collaboration responses where the given federation is the receiver.
    """
    try:
        url = (
            f"{CONTEXT_BROKER_URL}?type=CollaborationResponse"
            f"&q=responseStatus==ok&q=receiver==%22{federation_id}%22"
            f"&attrs=sender,senderAddress&options=keyValues"
        )
        response = requests.get(url, headers={"Content-Type": "application/json"})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching response for federation receiver: {e}")
        return None

def fetch_response_by_federation(federation_id,current_federation_id_in_collab,publisherID):
    """
    Fetch all federations in collaboration with the given federation.
    """
    federations_in_collab_with = {}
    try:
        url = (
            f"{CONTEXT_BROKER_URL}?type=CollaborationResponse"
            f"&q=responseStatus==ok&q=sender==%22{federation_id}%22"
            f"&attrs=receiver,responseTo&options=keyValues"
        )
        response = requests.get(url, headers={"Content-Type": "application/json"})
        response.raise_for_status()
        responses = response.json()

        if not isinstance(responses, list):
            responses = [responses]

        for response in responses:
            federation_with_collab_id = response.get("receiver")
            request_id = response.get("responseTo")

            if federation_with_collab_id and request_id:
                try:
                    url_get = f"{CONTEXT_BROKER_URL}/{request_id}?attrs=senderAddress&options=keyValues"
                    response_get = requests.get(url_get, headers={"Content-Type": "application/json"})
                    response_get.raise_for_status()
                    sender_data = response_get.json()

                    if isinstance(sender_data, list):
                        sender_data = sender_data[0]

                    sender_address = sender_data.get("senderAddress")
                    if sender_address:
                        federations_in_collab_with[federation_with_collab_id] = sender_address
                except requests.exceptions.RequestException as e:
                    # print(f"Error fetching request details: {e}")
                    continue
    except requests.exceptions.RequestException as e:
        print(f"Error fetching federation collaborations: {e}")
        return None

    # Fetch additional responses where the given federation is the receiver
    additional_responses = fetch_response_by_federation_receiver(federation_id)
    if additional_responses:
        if not isinstance(additional_responses, list):
            additional_responses = [additional_responses]

        for response in additional_responses:
            sender = response.get("sender")
            sender_address = response.get("senderAddress")
            if sender and sender_address:
                federations_in_collab_with[sender] = sender_address
    for key in [current_federation_id_in_collab, publisherID]:
        if key in federations_in_collab_with:
            federations_in_collab_with.pop(key)

    return federations_in_collab_with

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




def store_policy(policy):
    if not policy:
        print("Policy is None or empty, skipping storage.")
        return

    # Convert policy to dictionary if it is a string
    if isinstance(policy, str):
        try:
            policy = json.loads(policy)
        except json.JSONDecodeError:
            print("Failed to parse policy JSON string.")
            return

    headers = {'Content-Type': 'application/json'}
    policy_id = policy.get('id')
    if not policy_id:
        print("Policy ID is missing, cannot store policy.")
        return

    print(f"Processing policy with ID: {policy_id}")

    # Extract incoming lastModified timestamp
    incoming_last_modified_str = policy.get('modificationPolicy', {}).get('value', {}).get('lastModified')
    if not incoming_last_modified_str:
        print("Incoming policy does not have a lastModified timestamp.")
        return

    try:
        incoming_last_modified = datetime.strptime(incoming_last_modified_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        print(f"Invalid lastModified format: {incoming_last_modified_str}")
        return

    try:
        # Retrieve existing policy if it exists
        response = requests.get(f"{CONTEXT_BROKER_URL}/{policy_id}")
        if response.status_code == 200:
            existing_policy = response.json()
            existing_last_modified_str = existing_policy.get('modificationPolicy', {}).get('value', {}).get('lastModified')

            if existing_last_modified_str:
                try:
                    existing_last_modified = datetime.strptime(existing_last_modified_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                except ValueError:
                    print(f"Invalid lastModified format in existing policy: {existing_last_modified_str}")
                    return

                # Compare timestamps
                if existing_last_modified >= incoming_last_modified:
                    print(f"Existing policy {policy_id} is up-to-date. No changes made.")
                    return

                print(f"Incoming policy is newer. Updating policy {policy_id}.")
                # Delete the existing policy
                delete_response = requests.delete(f"{CONTEXT_BROKER_URL}/{policy_id}")
                delete_response.raise_for_status()
                print(f"Existing policy {policy_id} deleted successfully.")

        elif response.status_code != 404:
            print(f"Failed to retrieve existing policy {policy_id}. HTTP {response.status_code}")
            return

        # Store the incoming policy
        post_response = requests.post(CONTEXT_BROKER_URL, json=policy, headers=headers)
        post_response.raise_for_status()
        print(f"Policy {policy_id} stored successfully.")

    except requests.RequestException as e:
        print(f"Failed to process policy {policy_id}: {e}")


def setup_bridge(SOURCE_BROKER,SOURCE_PORT,DESTINATION_BROKERS,SOURCE_TOPIC):
    clients = bridge.setup_brokers(
    source_broker=SOURCE_BROKER,
    source_port=SOURCE_PORT,
    dest_brokers=DESTINATION_BROKERS,
    topics=SOURCE_TOPIC
    )
    # try:
    #     while True:
    #         time.sleep(1)
    # except KeyboardInterrupt:
    #     print("Shutting down...")
    #     for client in clients:
    #         client.loop_stop()
    #         client.disconnect()
    

def Policy_monitoring():
    policy_client = mqtt.Client(client_id=f"policy_{uuid4()}",protocol=mqtt.MQTTv5)

    policy_topic=f"Federation/+/Policy/#"
    # print(f"Policy topic: {policy_topic}")

    def forward_policies_to_federations(policy, topic, Publisher_Id):
        current_federation_id=topic.split("/")[1]
        policy_Id=topic.split("/")[-1]
        if not Publisher_Id:
            Publisher_Id=current_federation_id
        if current_federation_id==config.FEDERATION_ID:
            federations_in_collab_with=Policy_Federation_Mapping(current_federation_id,
                                                                policy_Id)
            # print(f"Federations in collaboration: {federations_in_collab_with}")
        else:
            federations_in_collab_with = fetch_response_by_federation(config.FEDERATION_ID, current_federation_id,Publisher_Id)
            # print(f"Federations in collaboration: {federations_in_collab_with}")
        destination_brokers = []

        if federations_in_collab_with:
            for federation_id, address in federations_in_collab_with.items():
                host, port = address.split(":")
                port = int(port)
                allow_bridging = validate_forwarding(policy, config.FEDERATION_ID.split(":")[-1], federation_id.split(":")[-1])

                if allow_bridging[0] and any(word in (allow_bridging[1] or "") for word in ["policy", "policies"]):
                    destination_brokers.append({"host": host, "port": port})

        return destination_brokers
                # bridge.setup_brokers(config.FED_BROKER, config.FED_PORT, destination_brokers, [topic])
    
    # Callback for remote policy client connection
    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            print("Connected successfully to remote MQTT Broker!")
            result, mid = client.subscribe(policy_topic, qos=1)
            if result == mqtt.MQTT_ERR_SUCCESS:
                print(f"Subscription request sent for topic: {policy_topic}")
            else:
                print(f"Subscription request failed with error code: {result}")
        else:
            print(f"Failed to connect to remote broker, return code {rc}")


    # Callback for subscription acknowledgment
    def on_subscribe(client, userdata, mid, granted_qos,properties=None):
        print(f"Subscription acknowledged with mid: {mid}, QoS: {granted_qos}")

    # Callback for receiving messages
    def on_message_policy(client, userdata, msg):
        # print(f"Message received on topic {msg.topic}: {msg.payload.decode('utf-8')}")
        # Received_time=time.time_ns()
        # Process(target=register_start_times,args=(Received_time,)).start()
        try:
            policy = json.loads(msg.payload.decode('utf-8'))
            # policy=json.dumps(policy)
            # Start a process to store policy if needed
            Process(target=store_policy, args=(policy,)).start()
            topic = [msg.topic]
            # print(f"Retrieved policy: {json.dumps(policy, indent=2)}")
            # Check if properties are available and retrieve PublisherID
            if msg.properties and hasattr(msg.properties, "UserProperty"):
                for key, value in msg.properties.UserProperty:
                    if key == "PublisherID":
                        publisher_ID=value
                        # print(f"PublisherID: {value}")
                        break
            else:
                publisher_ID = None
                # print("No PublisherID found in message properties.")

            # Publish to the local broker
            try:
                if isinstance(policy,str):
                    # print(policy)
                    policy=json.loads(policy)
                destination_brokers=forward_policies_to_federations(policy, msg.topic, publisher_ID)
                if destination_brokers:
                    setup_bridge(config.FED_BROKER,
                                                    config.FED_PORT,
                                                    destination_brokers,topic)
                    # Process(target=setup_bridge,args=(config.FED_BROKER,
                    #                                 config.FED_PORT,
                    #                                 destination_brokers,topic)).start()
            except json.JSONDecodeError as e:
                print(f"Failed to decode message payload: {e}")
        except Exception as e:
            print(f"Error processing the policy message: {e}")

    # Assign callbacks
    policy_client.on_connect = on_connect
    policy_client.on_subscribe = on_subscribe
    policy_client.on_message = on_message_policy

    try:
        # Connect to the remote broker
        policy_client.connect(config.FED_BROKER, int(config.FED_PORT), keepalive=60)
        policy_client.loop_forever()
    except Exception as e:
        print(f"An error occurred with the policy client: {e}")
    # finally:
    #     policy_client.loop_stop()
    #     policy_client.disconnect()


def main():
    print("Hello, Policy Synchronizer!")
    Policy_monitoring()

if __name__ == "__main__":
    main()