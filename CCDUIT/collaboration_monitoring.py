import paho.mqtt.client as mqtt
import json
from multiprocessing import Process
from uuid import uuid4
from datetime import datetime, timezone
import requests
import config as config
import time
import Context_Management_Service as cm
import MQTT_Bridge as bridge
import threading
from threading import Event
import Policy_Management_Service
from concurrent.futures import ThreadPoolExecutor

# MQTT and Context Broker configurations from config.py
FED_BROKER = config.FED_BROKER
FED_PORT = config.FED_PORT
CONTEXT_BROKER_URL = config.CONTEXT_BROKER_URL
FEDERATION_ID = config.FEDERATION_ID

def publish_to_collab_broker(message_dict, mosquitto_address_collab, port_collab):
    """
    Publishes policies to the mosquit_collab broker for the corresponding topics in parallel.

    Args:
        message_dict (dict): A dictionary with topics as keys and policies as values.
        mosquitto_address_collab (str): Address of the collaborator's MQTT broker.
        port_collab (int): Port of the collaborator's MQTT broker.
    """

    def publish_policy(topic, policy):
        """
        Publishes a single policy to the given topic.
        """
        # Create a publisher client with MQTT v5 protocol
        publisher_client = mqtt.Client(client_id=f"publisher{uuid4()}", protocol=mqtt.MQTTv5)
        try:
            publisher_client.connect(mosquitto_address_collab, port_collab)
            # print("collab")
            print(f"{mosquitto_address_collab}:{port_collab}")
            # Create properties and add the PublisherID
            properties = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
            properties.UserProperty = [("PublisherID", config.FEDERATION_ID)]  # Add a unique PublisherID

            # Convert the policy to JSON
            payload = json.dumps(policy)
            # print(payload)
            # Publish the message with properties
            publisher_client.publish(topic, payload, retain=True, qos=1, properties=properties)
            print(f"Published to topic '{topic}' with PublisherID and policy.")
        except Exception as e:
            print(f"Error publishing to {topic}: {e}")
        finally:
            publisher_client.disconnect()

    # Use a thread pool for parallel publishing
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(publish_policy, topic, policy) for topic, policy in message_dict.items()]

    print("Publishing complete.")


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
#retuen dictionary containing the topic and the policy entity

def get_request_by_id(orion_ld_url, entity_id):
    """
    Fetches an entity by ID from the Orion-LD Context Broker.

    :param orion_ld_url: Base URL of the Orion-LD Context Broker (e.g., "http://localhost:1026")
    :param entity_id: The ID of the entity to fetch
    :return: The entity as a JSON object, or None if not found
    """
    headers = {
        "Accept": "application/json"
    }

    url = f"{orion_ld_url}/{entity_id}"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()  # Return the entity as a JSON object
        elif response.status_code == 404:
            print(f"Entity with ID '{entity_id}' not found.")
            return None
        else:
            print(f"Error: Received status code {response.status_code}")
            # print(response.text)
            return None
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None


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


def store_response(response):
    headers = {'Content-Type': 'application/json'}
    entity_id = response.get('id')  # Assuming the entity ID is included in the response payload

    if not entity_id:
        print("Response does not have an 'id' field. Cannot process.")
        return

    try:
        # Check if the entity already exists
        get_response = requests.get(f"{CONTEXT_BROKER_URL}/{entity_id}", headers=headers)
        if get_response.status_code == 200:  # Entity exists
            print(f"Entity {entity_id} already exists. Deleting it.")
            delete_response = requests.delete(f"{CONTEXT_BROKER_URL}/{entity_id}", headers=headers)
            delete_response.raise_for_status()
            print(f"Entity {entity_id} deleted successfully.")

        # Store the new response
        post_response = requests.post(CONTEXT_BROKER_URL, data=json.dumps(response), headers=headers)
        post_response.raise_for_status()
        print("Response stored successfully.")

    except requests.exceptions.HTTPError as http_err:
        if get_response.status_code == 404:  # Entity does not exist, proceed to store it
            print(f"Entity {entity_id} does not exist. Creating new entity.")
            try:
                post_response = requests.post(CONTEXT_BROKER_URL, data=json.dumps(response), headers=headers)
                post_response.raise_for_status()
                print("Response stored successfully.")
            except requests.exceptions.RequestException as post_err:
                print(f"Failed to store response: {post_err}")
        else:
            print(f"HTTP Error: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection Error: Unable to connect to {CONTEXT_BROKER_URL}. Details: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout Error: Request to {CONTEXT_BROKER_URL} timed out. Details: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Failed to process response: {req_err}")

def update_request_status(request_id, status):
    headers = {'Content-Type': 'application/json'}
    update = {
        "status": {"type": "Property", "value": status}
    }
    params = {'type': 'CollaborationRequest'}
    try:
        response = requests.patch(f"{CONTEXT_BROKER_URL}/{request_id}/attrs", data=json.dumps(update), headers=headers, params=params)
        response.raise_for_status()
        print(f"Request {request_id} status updated to {status}")
    except requests.exceptions.RequestException as e:
        print("Failed to update request status:", e)

def store_policy(policy):
    if isinstance(policy, str):
        try:
            policy = json.loads(policy)  # Convert JSON string to dictionary
        except json.JSONDecodeError as e:
            print(f"Failed to parse policy as JSON: {e}")
            policy = None

    if policy is None:
        print("Policy is None, skipping storage.")
        return

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
        response = requests.post(CONTEXT_BROKER_URL, data=json.dumps(policy), headers=headers)
        response.raise_for_status()
        print(f"Policy {policy_id} stored successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to process policy {policy_id}: {e}")

def generate_and_send_response(request, response_status,policies_topics, policy_ID=None):
    receiver_fed_id = request["sender"]["value"]
    response = {
        "id": f"urn:ngsi-ld:CollaborationResponse:{uuid4()}",
        "type": "CollaborationResponse",
        "sender": {"type": "Property", "value": FEDERATION_ID},
        "senderAddress": {"type": "Property", "value": f"{FED_BROKER}:{FED_PORT}"},
        "receiver": {"type": "Property", "value": receiver_fed_id},
        "responseTo": {"type": "Property", "value": request["id"]},
        "responseStatus": {"type": "Property", "value": response_status},
        "policiesTopics":{"type": "Relationship", "object": policies_topics},
        "timestamp": {"type": "Property", "value": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    }
    if response_status == "ok":
        response["policyID"] = {"type": "Property", "value": f"{policy_ID}"}
    sender_address = request["senderAddress"]["value"]
    sender_broker, sender_port = sender_address.split(":")

    Process(target=run_response_client, args=(sender_broker, sender_port, receiver_fed_id, response, request["id"], response_status)).start()

def run_response_client(sender_broker, sender_port, receiver_fed_id, response, request_id, response_status):
    response_id=response['id']
    try:
        response_client = mqtt.Client(client_id=f"response_{uuid4()}")
        response_client.connect(sender_broker, int(sender_port), 60)
        response_client.publish(f'{receiver_fed_id}/Collaboration/responses/{response_id}', json.dumps(response),retain=True)
        # print("Response sent:", json.dumps(response, indent=2))
        response_client.disconnect()
    except Exception as e:
        print("Failed to send response:", e)
    request_status = "active" if response_status.strip().lower() == "ok" else "refused"
    Process(target=update_request_status,args=(request_id,request_status)).start()
    Process(target=store_response, args=(response,)).start()

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
                    print(f"Error fetching request details: {e}")
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



def run_policy_client(broker, port, topic, publisher_id, message):
    print(f"run_policy_client({broker}, {port}, {topic}, {publisher_id}, {message})")
    """
    Publishes a message to an MQTT broker with a given Publisher ID in the properties.
    
    :param broker: MQTT broker address
    :param port: MQTT broker port
    :param topic: Topic to publish the message to
    :param publisher_id: Publisher ID to include in the message properties
    :param message: The message payload to publish
    """
    # Event to wait for message publication
    message_published_event = Event()

    # Callback for connection
    def on_connect(client, userdata, flags, rc,properties=None):
        if rc == 0:
            print("Connected to MQTT broker successfully!")
        else:
            print(f"Failed to connect, return code {rc}")

    # Callback for message publication
    def on_publish(client, userdata, mid):
        print(f"Message with mid {mid} published successfully!")
        message_published_event.set()

    # Initialize the client
    client = mqtt.Client(protocol=mqtt.MQTTv5)

    # Assign the callbacks
    client.on_connect = on_connect
    client.on_publish = on_publish

    # Connect to the broker
    try:
        client.connect(broker, port, keepalive=60)
    except Exception as e:
        print(f"Connection failed: {e}")


    # Start the client loop in a background thread
    client.loop_start()

    # Create the properties for the message
    properties = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
    properties.UserProperty = [("PublisherID", publisher_id)]

    # # Ensure the payload is a valid type (string or bytes)
    # if not isinstance(message, (str, bytes)):
    #     raise ValueError("Payload must be a string, bytes, or None")

    # Convert dictionary payloads to JSON
    # if isinstance(message, dict):
    #     import json
    #     message = json.dumps(message,indent=4)

    # Publish the message
    result, mid = client.publish(topic, payload=json.dumps(message), qos=1, properties=properties, retain=True)
    print(f"Message published to topic '{topic}' with PublisherID '{publisher_id}'")

    # Wait for the message to be published
    if not message_published_event.wait(timeout=1):  # Timeout to avoid indefinite waiting
        print("Publishing the message timed out.")
    
    # Stop the client loop and disconnect
    client.loop_stop()
    client.disconnect()


def on_message(client, userdata, msg):
    """
    Callback function triggered when a message is received on the subscribed topic.
    """
    try:
        policy = json.loads(msg.payload.decode())
        userdata['policies'].append(policy)  # Store only the policy entity
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")

def subscribe_retrieve_all_policies(base_topic, mosquitto_address, port=1883, timeout=0.3):
    """
    Subscribes to a base topic and retrieves all incoming messages.

    Args:
        base_topic (str): The base topic to subscribe to.
        mosquitto_address (str): The address of the MQTT broker.
        port (int): The port of the MQTT broker (default: 1883).
        timeout (int): Time (in seconds) to wait for messages before stopping (default: None for unlimited).

    Returns:
        list: A list of all policies received.
    """
    userdata = {'policies': []}
    client = mqtt.Client(userdata=userdata)
    client.on_message = on_message

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            wildcard_topic = f"{base_topic}#"
            client.subscribe(wildcard_topic)
            print(f"Subscribed to: {wildcard_topic}")

    client.on_connect = on_connect

    try:
        client.connect(mosquitto_address, port)
        client.loop_start()
        start_time = datetime.now()
        while timeout is None or (datetime.now() - start_time).total_seconds() < timeout:
            pass  # Wait for messages
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.loop_stop()
        client.disconnect()

    return userdata['policies']  # Return only the policies



def get_most_suitable_policy(federation_id, policies):
    """
    Retrieve the most suitable policy entity based on specified conditions.

    Args:
        federation_id (str): The Federation ID to evaluate.
        policies (list): A list of policy entities.

    Returns:
        dict or bool: The most suitable policy entity or False if no suitable policy is found.
    """
    # Helper function to score policies based on criteria
    def score_policy(policy):
        sharing_rules = policy.get("sharingRules", {}).get("value", [])
        permitted_types = policy.get("permittedContextTypes", {}).get("value", [])
        public_rule = next((rule for rule in sharing_rules if "public" in rule), None)

        # Check specific federation ID in sharingRules
        for rule in sharing_rules:
            if federation_id in rule:
                federation_rule = rule[federation_id]
                if not federation_rule.get("canReceive", False) or str(federation_rule.get("canForward", "")).lower() == "false":
                    return float('-inf')  # Reject this policy outright
                if (
                        federation_rule.get("canReceive", False) is True and federation_rule.get("canForward", False) is True
                    ) or (
                        str(federation_rule.get("canReceive", "")).lower() == "true" and str(federation_rule.get("canForward", "")).lower() == "true"
                    ):
                    return 3  # High priority for canReceive=True and canForward=True
                if str(federation_rule.get("canReceive", "")).lower() == "true" and str(federation_rule.get("canForward", "")).lower() == "false":
                    return 2  # Medium priority for canReceive=True only

        # Check "public" policies
        if public_rule:
            public_policy = public_rule["public"]
            if str(public_policy.get("canReceive", "")).lower() == "true" and str(public_policy.get("canForward", "")).lower() == "true":
                return 1.5  # Fallback priority for public with both permissions
            if str(public_policy.get("canReceive", "")).lower() == "true":
                match_count = len([t for t in permitted_types if t in public_policy])
                return 1 + 0.1 * match_count  # Slightly higher priority for more permittedContextTypes

        # No suitable match
        return 0
    # Evaluate all policies and return the highest scoring one
    scored_policies = [(score_policy(policy), policy) for policy in policies]
    scored_policies.sort(reverse=True, key=lambda x: x[0])  # Sort by score, descending
    # Return the highest scoring policy, or False if none are suitable
    return scored_policies[0][1] if scored_policies and scored_policies[0][0] > 0 else False

def subscribe_to_topics(mqtt_address, mqtt_port, topics):
    """
    Subscribes to a list of topics on an MQTT broker in parallel using a single client.

    Args:
        mqtt_address (str): Address of the MQTT broker.
        mqtt_port (int): Port of the MQTT broker.
        topics (list): List of topic strings to subscribe to.

    Returns:
        None
    """
    
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print(f"Connected to MQTT broker at {mqtt_address}:{mqtt_port}")
        else:
            print(f"Connection failed with code {rc}")

    def on_message(client, userdata, msg):
        policy=json.dumps(json.loads(msg.payload.decode('utf-8')))
        Process(target=store_policy, args=(policy,)).start()

    def subscribe_to_topic(client, topic):
        """Subscribe to a single topic in a thread."""
        client.subscribe(topic)
        print(f"Subscribed to topic: {topic}")

    # Create a single client
    client = mqtt.Client(client_id=f"Policies_topics_subscriber{uuid4()}")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(mqtt_address, mqtt_port, 60)

    # Create threads for subscribing to topics
    threads = []
    for topic in topics:
        thread = threading.Thread(target=subscribe_to_topic, args=(client, topic))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete (optional)
    for thread in threads:
        thread.join()

    # Start the loop to process MQTT messages
    client.loop_forever()


def on_request_message(client, userdata, message):
    request = json.loads(message.payload)
    federation_id=request["sender"]["value"]
    # print("Received request:", json.dumps(request, indent=2))
    Process(target=store_request, args=(request,)).start()
    # store_request(request)
    policies=subscribe_retrieve_all_policies(f"Federation/{config.FEDERATION_ID}/Policy/",
                                             config.FED_BROKER,config.FED_PORT,timeout=1)
    policy=get_most_suitable_policy(federation_id,policies)
    print(f"get_most_suitable_policy:{policy}")
    if policy:
        # if isinstance(policy, str):
        #     policy=json.loads(policy)
        # policy=json.dumps(policy)
        response_status = "ok"
        sender_address = request["senderAddress"]["value"]
        sender_broker, sender_port = sender_address.split(":")
        policy_ID = request["policyID"]["value"]
        sender_ID = request["sender"]["value"]
        policy_topic = f"Federation/{sender_ID}/Policy/{policy_ID}"
        sub_topics=request["policiesTopics"]["object"]
        pub_policy_id=policy['id']
        pub_policy_topic=f"Federation/{config.FEDERATION_ID}/Policy/{pub_policy_id}"
        # if isinstance(policy, str):
        #     policy = json.loads(policy)
        Process(target=run_policy_client, args=(sender_broker, int(sender_port), pub_policy_topic,config.FEDERATION_ID,policy)).start()
        # if sub_topics:
        #     Process(target=subscribe_to_topics, args=(config.FED_BROKER, int(config.FED_PORT), sub_topics)).start()
            
        topics_dict=create_policy_topics_2_new_collaborators(config.FEDERATION_ID,sender_ID,
                                                            config.FED_BROKER,config.FED_PORT)
        if topics_dict:
            Process(target=publish_to_collab_broker, args=(topics_dict,sender_broker ,int(sender_port))).start()
        policies_topics=list(topics_dict.keys())
        
    else:
        response_status = "no"

    generate_and_send_response(request, response_status,policies_topics,policy_ID=pub_policy_id)

def on_response_message(client, userdata, message):
    response = json.loads(message.payload)
    # print("Received response:", json.dumps(response, indent=2))
    Process(target=store_response, args=(response,)).start()
    response_status = response["responseStatus"]["value"]
    request_id = response["responseTo"]["value"]
    request= get_request_by_id(config.CONTEXT_BROKER_URL,request_id)
    # print(request)
    if response_status == "ok":
        update_request_status(request_id, "active")
        sender_address = response["senderAddress"]["value"]
        sender_broker, sender_port = sender_address.split(":")
        policy_ID = response["policyID"]["value"]
        sender_ID = response["sender"]["value"]
        policy_topic = f"Federation/{sender_ID}/Policy/{policy_ID}"
        # sub_topics = request.get("policiesTopics", {}).get("object", None) if request else None
        pub_policy_id=request["policyID"]["value"]
        pub_policy_topic=f"Federation/{config.FEDERATION_ID}/Policy/{pub_policy_id}"
        policy= Policy_Management_Service.subscribe_retrieve_policy(pub_policy_topic,config.FED_BROKER,
                                                                    int(config.FED_PORT),timeout=0.5)
        # policy=json.dumps(policy)
        Process(target=run_policy_client, args=(sender_broker, int(sender_port), pub_policy_topic,config.FEDERATION_ID,policy)).start()
        # if sub_topics:
        #     Process(target=subscribe_to_topics, args=(config.FED_BROKER, int(config.FED_PORT), sub_topics)).start()
            
        topics_dict=create_policy_topics_2_new_collaborators(config.FEDERATION_ID,sender_ID,
                                                            config.FED_BROKER,config.FED_PORT)
        if topics_dict:
            Process(target=publish_to_collab_broker, args=(topics_dict,sender_broker ,int(sender_port))).start()
    elif response_status == "no":
        update_request_status(request_id, "refused")

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    subscriptions = [
        (f"{FEDERATION_ID}/Collaboration/requests/#", 0),
        (f"{FEDERATION_ID}/Collaboration/responses/#", 0)
    ]
    sub_result, mid = client.subscribe(subscriptions)
    if sub_result == 0:
        print(f"Subscribed to topics with message ID {mid}")
    else:
        print(f"Failed to subscribe to topics. Return code: {sub_result}")

def on_disconnect(client, userdata, rc):
    print("Disconnected with result code " + str(rc))
    while rc != 0:
        time.sleep(5)
        try:
            rc = client.reconnect()
            print("Reconnected with result code " + str(rc))
        except Exception as e:
            print("Failed to reconnect:", e)

client = mqtt.Client(client_id=f"main_{uuid4()}")
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_request_message
client.message_callback_add(f"{FEDERATION_ID}/Collaboration/requests/#", on_request_message)
client.message_callback_add(f"{FEDERATION_ID}/Collaboration/responses/#", on_response_message)

# def run_client():
#     client.connect(FED_BROKER, FED_PORT, 60)
#     client.loop_forever()

def clear_retained_messages(client, topics):
    """
    Clear retained messages for the given topics by publishing an empty payload with retain=True.
    """
    for topic in topics:
        client.publish(topic, payload="", qos=0, retain=True)
        print(f"Cleared retained messages for topic: {topic}")

def run_client():
    client.connect(FED_BROKER, FED_PORT, 60)
    
    # Clear retained messages for specific topics
    topics_to_clear = [
        f"{FEDERATION_ID}/Collaboration/requests",
        f"{FEDERATION_ID}/Collaboration/responses"
    ]
    clear_retained_messages(client, topics_to_clear)

    # Start the client loop
    client.loop_forever()


def main():
    # Your main program logic here
    print("Hello, collaboration monitoring!")
    Process(target=run_client).start()

if __name__ == "__main__":
    main()
