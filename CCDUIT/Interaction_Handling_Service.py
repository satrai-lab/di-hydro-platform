from pickle import NONE
import json
from rdflib import Graph
import uuid
import multiprocessing
import paho.mqtt.client as mqtt
import requests
import time
import hashlib
from flask_cors import CORS
import os
import signal
import config as config
import json
import requests
import Context_Management_Service
import functools
import Function_Management_Service
import psutil
from datetime import datetime, timedelta
import hashlib
import functools
import paho.mqtt.client as mqtt
import threading
import sys

import queue
message_queue = multiprocessing.Queue()

# sys.path.append(config.FUNCTION_REPOSITORY_PATH)
import Function_Repository


context_broker_url = config.CONTEXT_BROKER_URL
context_url="https://raw.githubusercontent.com/NiematKhoder/test/main/Context.json"

headers = {'Content-Type': 'application/ld+json'}
link_header_value = f'<{json.dumps(context_url).replace(" ", "")}>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
headersget = {
    "Accept": "application/ld+json",  # Request JSON-LD format
    "Link": f'<{context_url}>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
}


def monitor_memory_usage(pid):
    """
    Monitor memory usage of the interaction process with the given PID,
    and save cumulative memory usage in MB to a specified text file when the process terminates.
    
    Parameters:
    - pid: Process ID to monitor
    - filename: Name of the file where memory usage details will be saved
    """
    filename=f"memory_usage_log_{pid}.txt"
    try:
        process = psutil.Process(pid)
        
        # Initialize cumulative memory counter
        total_memory_usage_mb = 0
        sample_count = 0
        
        # Open the file to log memory usage details
        with open(filename, "w") as file:
            file.write(f"Monitoring memory usage for process ID: {pid}\n")
            file.write("Time (s), RSS Memory (MB)\n")

            # Loop until the process terminates
            while process.is_running() and not process.status() == psutil.STATUS_ZOMBIE:
                memory_info = process.memory_info()
                rss_memory_mb = memory_info.rss / 10**6  # Convert from bytes to MB
                current_time = time.time()
                
                # Log the memory usage in the text file
                file.write(f"{current_time:.2f}, {rss_memory_mb:.2f}\n")
                print(f"Process ID: {pid}, RSS Memory: {rss_memory_mb} MB")

                # Accumulate memory usage and increment sample count
                total_memory_usage_mb += rss_memory_mb
                sample_count += 1
                
                time.sleep(1)  # Adjust frequency of checks as needed

            # Calculate average memory usage
            average_memory_usage_mb = total_memory_usage_mb / sample_count if sample_count else 0

            # Log cumulative and average memory usage in the file
            file.write(f"\nProcess {pid} terminated.\n")
            file.write(f"Total accumulated RSS memory usage: {total_memory_usage_mb:.2f} MB\n")
            file.write(f"Average RSS memory usage: {average_memory_usage_mb:.2f} MB\n")
        
        print(f"Process {pid} terminated.")
        print(f"Total accumulated RSS memory usage: {total_memory_usage_mb:.2f} MB")
        print(f"Average RSS memory usage: {average_memory_usage_mb:.2f} MB")
    
    except psutil.NoSuchProcess:
        print(f"Process with PID {pid} not found.")


def get_endpoint_url(community_id):
    """
    Fetch the endpoint URL for the given community from the database.
    """
    community = Context_Management_Service.get_community_by_id(community_id)
    # print("HERE IS AN ENDPOINT!!!!!!!!!!!!")
    # print(json.dumps(community,indent=2))
    if community and 'connectionDetails' in community:
        connection_details = community['connectionDetails'].get('value', {})
        if 'endpoint' in connection_details:
            return str(connection_details['endpoint'])
    return None

def get_protocol(community_id):
    """
    Fetch the protocol for the given community from the database.
    """
    community = Context_Management_Service.get_community_by_id(community_id)

    # print("HERE IS AN ENDPOINT!!!!!!!!!!!!")
    # print(json.dumps(community,indent=2))
    
    if community and 'connectionDetails' in community:
        connection_details = community['connectionDetails'].get('value', {})
        if 'protocol' in connection_details:
            return str(connection_details['protocol'])
    return None

def get_Converter_name(function_ID):
    prefix = "urn:ngsi-ld:Function:"
    if function_ID.startswith(prefix):
        function_ID = function_ID[len(prefix):]
    converter = Function_Management_Service.get_function_by_id(function_ID)
    # print(json.dumps(converter,indent=2))
    if converter and 'callFunction' in converter:
        converter_name = converter['callFunction'].get('value', {})
        return converter_name
    return None

def find_mapping(source_model, dest_model ):
    # maping_time=time.perf_counter_ns()
    mapping = Function_Management_Service.check_data_model_mapping(source_model, dest_model)
    # print(f"mapping time:{(time.perf_counter_ns()-maping_time)/1_000_000}")
    if not mapping:
        print("Mapping doesn't exist. Please provide a mapping")
        #here we should go and ask the fedeartions about the converters
        return None

    if isinstance(mapping, list):
        # print("Indirect mapping found:")
        for func in mapping:
            print(f"  - {func['id']}")
        #there should be a code here to handel the indirect mapping
    else:
        function_ID=mapping['id']
        # print(f"Direct mapping found: {function_ID}")
        converter_name=get_Converter_name(function_ID)
        if converter_name is None:
            # Handle the None case, e.g., log an error or assign a default value
            print("Error: converter_name is None")
            return None
        else:
            return converter_name
        

def convert_data(data,converter_name):
    """
    Convert data from source model to destination model.
    """
    if hasattr(Function_Repository, converter_name):
        converter_func = getattr(Function_Repository, converter_name)
        if callable(converter_func):
            # conversion=time.perf_counter_ns()
            converted_data = converter_func(data)
            # print(f"conversion time in converter function :{(time.perf_counter_ns()-conversion)/1_000_000}")
            # print(f"Converted data: {converted_data}")
            return converted_data
        else:
            print(f"The attribute '{converter_name}' in Function_Repository is not callable.")
            return None
    else:
        print(f"Function_Repository has no attribute named '{converter_name}'.")    
        return None
        
def compute_data_hash(data):
    """
    Compute the MD5 hash of the given data.
    """
    return hashlib.md5(str(data).encode()).hexdigest()


def log_time(delay_ms,filename="time_log.txt"):
    """Logs delay time to a file asynchronously."""
    try:
        with open(f"{filename}", 'a') as file:
            file.write(f"{delay_ms}\n")
    except Exception as e:
        print(f"Error writing to file: {e}")

def http_worker(community_context_url, message_queue,startup_time):
    while True:
        try:
            data = message_queue.get()  # This will block until there is data
            if data is None:
                break  # Stop the process if None is received (graceful exit)

            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json"
            }

            try:

                response =requests.post(community_context_url, json=data["converted_data"], headers=headers)
                # print(f"posting time {(time.perf_counter_ns() - before_posting) / 1_000_000} ms ")
                if response.status_code in [200, 201]:
                    # end_time=time.perf_counter_ns()
                    # startup_delay=(end_time - data["timestamp_ns"]) / 1_000_000
                    # delay_ms = (tend_time - data["timestamp_ns"]) / 1_000_000
                    # threading.Thread(target=log_time, args=(startup_delay,"startup_log_brick_ngsild.txt")).start()
                    # threading.Thread(target=log_time, args=(delay_ms,)).start()
                    print(f"HTTP POST successful to {community_context_url}")

            except Exception as e:
                print("HTTP request failed:", e)

        except Empty:
            continue  # Avoid blocking the process indefinitely

def on_message(client, userdata, message, target_data_model, dest_mqtt_client, destpath, destination_protocol, destination_endpoint,
                same_data_model,converter_name,startup_time):
    """
    Callback function for handling messages from source MQTT client, converting data if necessary,
    and sending to the destination endpoint (either MQTT or HTTP).
    """
    # start_time1=time.perf_counter_ns()
    
    source_data = message.payload.decode(errors='ignore')

    # Convert data model if required
    if not same_data_model:
        converted_data = convert_data(source_data, converter_name)
        if converted_data is None:
            print("Data conversion failed.")
            return
    else:
        converted_data = source_data
    if  isinstance(converted_data, list):
                converted_data=converted_data[0]
                
    if destination_protocol.lower() == "http":
        message_with_timestamp = {
            "converted_data": converted_data,
            "timestamp_ns": start_time1
        }
        # Put the dictionary in the queue
        message_queue.put(message_with_timestamp)

    elif destination_protocol.lower() == "mqtt" and dest_mqtt_client:
        # print(f"Publishing data to MQTT topic {destpath}")
            try:
                result = dest_mqtt_client.publish(destpath, json.dumps(converted_data))
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    # end_time=time.perf_counter_ns()
                    # delay_ms = (end_time - start_time1) / 1_000_000  # Convert to milliseconds
                    # statup_delay=(end_time-startup_time)/1_000_000
                    # # Logging in a separate thread
                    # threading.Thread(target=log_time, args=(statup_delay,"startup_log_mqtt_mqtt.txt")).start()
                    # threading.Thread(target=log_time, args=(delay_ms,)).start()

                    print("Data successfully published to MQTT.")
                else:
                    # print(f"End time: {time.perf_counter_ns()}")
                    print(f"Failed to publish message, MQTT error code: {result.rc}")
            except Exception as e:
                # print(f"End time: {time.perf_counter_ns()}")
                print(f"MQTT publishing error: {e}")


def interaction_process(interaction_id, source_community, destination_community, Interaction_Type,
                        source_data_model, target_data_model, sourcepath, destpath,same_data_model,converter_name):
    """
    Process the interaction between source and destination communities with real-time status updates.
    """
    print(f"Starting interaction process with ID: {interaction_id}")

    # Set up source and destination endpoints and protocols
    source_endpoint = get_endpoint_url(source_community)
    destination_endpoint = get_endpoint_url(destination_community)
    source_protocol = get_protocol(source_community)
    destination_protocol = get_protocol(destination_community)

    # Track interaction status and set processing interval
    interaction_status = "active"
    processing_active = True  # Flag to control data processing
    last_fetch_time = datetime.now() - timedelta(seconds=3)  # Initialize to fetch immediately

    # Callback function for receiving interaction status updates via MQTT
    def on_status_message(client, userdata, message):
        nonlocal interaction_status, processing_active
        interaction_status = message.payload.decode().lower()
        print(f"Received interaction status update: {interaction_status}")
        
        # Control processing based on status update
        if interaction_status == "pause":
            print("Interaction is paused, waiting for resume signal...")
            processing_active = False
        elif interaction_status in ["active", "resume"]:
            print("Interaction is active, processing data...")
            processing_active = True

    # Set up MQTT client for subscribing to interaction status updates
    mqtt_client = mqtt.Client()
    mqtt_client.on_message = on_status_message
    mqtt_client.connect(config.FED_BROKER, config.FED_PORT, 60)
    
    # Subscribe to the topic for interaction status updates
    status_topic = f"interaction/status/{interaction_id}"
    mqtt_client.subscribe(status_topic)
    mqtt_client.loop_start()
    # print(f"Subscribed to MQTT topic for status updates: {status_topic}")

    previous_data_hash = None
    dest_mqtt_client = None

    # Destination MQTT setup if applicable
    if destination_protocol.lower() == "mqtt":
        dest_mqtt_client = mqtt.Client()
        host, port = destination_endpoint.split(":")
        dest_mqtt_client.connect(host, int(port), 60)
        dest_mqtt_client.loop_start()

        
    if source_protocol.lower()=="mqtt" and destination_protocol.lower() == "http":  
        community_context_url=f"{destination_endpoint}{destpath}"
        p = multiprocessing.Process(target=http_worker, args=(community_context_url,message_queue,startup_time),daemon=True)
        p.start()
        
    # If source uses MQTT protocol, set up client and subscribe
    if source_protocol.lower() == "mqtt":
        source_mqtt_client = mqtt.Client(userdata={'source_data_model': source_data_model})
        customized_on_message = functools.partial(on_message, 
                                                  target_data_model=target_data_model, 
                                                  dest_mqtt_client=dest_mqtt_client, 
                                                  destpath=destpath, 
                                                  destination_protocol=destination_protocol, 
                                                  destination_endpoint=destination_endpoint,
                                                  same_data_model=same_data_model,
                                                  converter_name=converter_name,
                                                  startup_time=startup_time)
        source_mqtt_client.on_message = customized_on_message
        source_mqtt_address, source_mqtt_port = source_endpoint.split(':')
        source_mqtt_client.connect(source_mqtt_address, int(source_mqtt_port), 60)
        print(f"MQTT CLIENT IS CONNECTED to {source_endpoint}")
        source_mqtt_client.subscribe(sourcepath)
        while processing_active:
            source_mqtt_client.loop_start()

    # Main loop for processing data when active
    while True:
        # Only process if active and time interval has passed
        if processing_active and datetime.now() >= last_fetch_time + timedelta(seconds=0.9):
            last_fetch_time = datetime.now()  # Update last fetch time

            # Record the start time for this fetch/processing cycle
            # start_time = time.perf_counter_ns()
            

            # If source protocol is HTTP, fetch and process data
            if source_protocol.upper() == "HTTP":
                source_endpoint_with_path = str(source_endpoint) + str(sourcepath)
                response = requests.get(source_endpoint_with_path)

                if response.status_code == 200 and response.content:
                    source_data = response.json()
                    if isinstance(source_data, list) and source_data:
                        source_data = source_data[0]
                    if not source_data:
                        print("No data available in the response.")
                        continue
                    
                    # Check if fetched data has changed since last iteration
                    current_data_hash = compute_data_hash(source_data)
                    if current_data_hash == previous_data_hash:
                        print("No change in data since last fetch.")
                        continue  # Skip if data hasn't changed

                    previous_data_hash = current_data_hash
                    
                    # Convert data if necessary
                    if not same_data_model:
                        print("Converting data between models.")
                        converted_data = convert_data(source_data, converter_name)
                        if converted_data is None:
                            print("Failed to convert the received data.")
                            continue
                    else:
                        converted_data = source_data
                    
                    # Send converted data to the destination based on its protocol
                    
                    if destination_protocol.lower() == "http":
                        destination_endpoint_with_path=f"{destination_endpoint}{destpath}"
                        print(f"Sending data to HTTP endpoint: {destination_endpoint_with_path}")
                        try:
                            headers = {
                                "Content-Type": "application/json",
                                "Accept": "application/json"
                            }

                            response = requests.post(destination_endpoint_with_path, json=converted_data, headers=headers)
                            if response.status_code in [200, 201]:
                                # end_time=time.perf_counter_ns()
                                # delay_ms = (end_time - start_time) / 1_000_000
                                # statup_delay=(end_time-startup_time)/1_000_000
                                # # Write to file in a separate thread
                                # threading.Thread(target=log_time, args=(statup_delay,"startup_log_ngsild_ngsild.txt")).start()
                                # threading.Thread(target=log_time, args=(delay_ms,)).start()

                                print("Data successfully sent via HTTP.")
                            else:
                                # print(f"End time: {time.perf_counter_ns()}")
                                print(f"Failed to send data via HTTP. Status code: {response.status_code}")
                        except Exception as e:
                            # print(f"End time: {time.perf_counter_ns()}")
                            print(f"HTTP request error: {e}")
                        # send_over_http(destination_endpoint_with_path,converted_data,start_time)

                    elif destination_protocol.lower() == "mqtt" and dest_mqtt_client:
                            try:
                                result = dest_mqtt_client.publish(destpath, json.dumps(converted_data))
                                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                                    # end_time=time.perf_counter_ns()
                                    # delay_ms = (end_time - start_time) / 1_000_000  # Convert to milliseconds
                                    # statup_delay=(end_time-startup_time)/1_000_000
                                    # # Write to file in a separate thread
                                    # threading.Thread(target=log_time, args=(statup_delay,"startup_log_ngsild_brick.txt")).start()
                                    # # Logging in a separate thread
                                    # threading.Thread(target=log_time, args=(delay_ms,)).start()
                        
                                    print("Data successfully published to MQTT.")
                                else:
                                    # print(f"End time: {time.perf_counter_ns()}")
                                    print(f"Failed to publish message, MQTT error code: {result.rc}")
                            except Exception as e:
                                # print(f"End time: {time.perf_counter_ns()}")
                                print(f"MQTT publishing error: {e}")
                        # publish_to_mqtt(dest_mqtt_client, str(destpath), converted_data, start_time)
                else:
                    print(f"Failed to fetch data from {source_endpoint_with_path}. Status code: {response.status_code}")


def fetch_policy_by_provider_federation(provider_federation_id):
    """
    Fetch a policy directly from the context broker based on the provider federation ID.
    """
    import requests

    headers = {
        "Content-Type": "application/ld+json"
    }

    try:
        url=f"{context_broker_url}?q=providerFederation==%22{provider_federation_id}%22"
        
        response = requests.get(
        f'{url}',headers={"Content-Type": "application/json"}
    )
        response.raise_for_status()
        
        # Parse the response to extract the policy
        policies = response.json()
        if isinstance(policies,list):
            policies=policies[0]
        # print(policies)
        return policies
    except requests.exceptions.RequestException as e:
        print(f"Error fetching policy by provider federation: {e}")
        return None


def validate_Interaction(interaction_entity,federation_id):
    """
    Ensures interactions adhere to set policies.
    """
    
    # Fetch the policy
    policy = fetch_policy_by_provider_federation(federation_id)
    # print(json.dumps(policy,indent=2))
    if not policy:
        print("Policy not found!")
        return False

    # Validate permitted context types
    # permitted_types = policy.get("permittedContextTypes", {}).get("value", [])
    permitted_types = policy.get("permittedContextTypes", {}).get("value", [])
    # print(permitted_types)
    # print(interaction_entity["connectionType"]["value"])
    if interaction_entity["connectionType"]["value"] not in permitted_types:
        # print("Interaction type not permitted by policy!")
        return False

    # Validate sharing rules
    sharing_rules = policy["sharingRules"]["value"]
    # print(sharing_rules)
    destination_federation =config.FEDERATION_ID.split(":")[-1]
    # print(destination_federation)
    
    if not isinstance(sharing_rules, list):
        sharing_rules = [sharing_rules]
        # print(f"{sharing_rules}") 
    allowed_to_share = False
    for rule  in sharing_rules:
        # print(f" rule: {rule}")
        # print("before if")
        if destination_federation in  rule: 
            # print("if destination_federation in  rule:")  
            # print(rule[destination_federation]["canReceive"])
            if rule[destination_federation]["canReceive"]==True or rule[destination_federation]["canReceive"].lower()=="true":   # Handle "true" (str) or True (bool)
                allowed_to_share = True
        if "public".lower() in  rule: 
            if rule["public"]["canReceive"]==True or rule["public"]["canReceive"].lower()=="true":   # Handle "true" (str) or True (bool)
                allowed_to_share = True
    
        if allowed_to_share:  # Break early if allowed
            break
    if not allowed_to_share:
        print("Sharing rules do not permit this interaction!")

    else:
        print("Sharing is allowed!")

        print("Interaction validation succeeded!")
        return True,policy


def validate_Recieving(policy, federationID):
    # Access the sharing rules and permitted context types from the policy
    sharing_rules = policy.get("sharingRules", {}).get("value", [])
    permitted_context_types = policy.get("permittedContextTypes", {}).get("value", [])
    
    # Flags for the federation conditions
    federation_can_receive = False
    federation_exists = False

    # Debugging print statements
    # print(f"[validate_Recieving] Policy ID: {policy.get('id')}")
    # print(f"[validate_Recieving] Federation ID: {federationID}")
    # print(f"[validate_Recieving] Sharing Rules: {sharing_rules}")
    # print(f"[validate_Recieving] Permitted Context Types: {permitted_context_types}")
    
    # Loop through each federation in the sharing rules
    for federation_rule in sharing_rules:
        for federation_id, permissions in federation_rule.items():
            # print(f"[validate_Recieving] Checking federation_rule: {federation_rule}")
            if federation_id == federationID:
                print(f"[validate_Recieving] Federation {federationID} found in sharing rules.")
                # Check if federationID has canReceive permission
                if permissions["canReceive"] == True or str(permissions["canReceive"]).lower() == "true": 
                    federation_can_receive = True
                    federation_exists = True
                    # print(f"[validate_Recieving] Federation {federationID} has 'canReceive' permission.")

    if federation_exists:
        # print(f"[validate_Recieving] Federation exists. Checking permitted context types...")
        if "communities" or "community" in permitted_context_types:
            # print("[validate_Recieving] 'communities' or 'community' found in permitted context types.")
            return True, permitted_context_types
    elif federation_can_receive:
        # print(f"[validate_Recieving] Federation {federationID} can receive but does not exist in sharing rules.")
        return True, permitted_context_types
    else:
        # print(f"[validate_Recieving] Federation {federationID} cannot receive. Returning False.")
        return False, permitted_context_types


def fetch_request_by_federation_sender(federation_id):
    """
    Fetch collaboration responses where the given federation is the receiver.
    """
    try:
        url = (
            f"{context_broker_url}?type=CollaborationRequest"
            f"&q=status==active&q=sender==%22{federation_id}%22"
            f"&attrs=senderAddress&options=keyValues"
        )
        response = requests.get(url, headers={"Content-Type": "application/json"})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching response for federation receiver: {e}")
        return None


def fetch_response_by_federation_sender(federation_id):
    """
    Fetch collaboration responses where the given federation is the receiver.
    """
    try:
        url = (
            f"{context_broker_url}?type=CollaborationResponse"
            f"&q=responseStatus==ok&q=sender==%22{federation_id}%22"
            f"&attrs=senderAddress&options=keyValues"
        )
        response = requests.get(url, headers={"Content-Type": "application/json"})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching response for federation receiver: {e}")
        return None


# monitor_Interaction function that utilizes validate_Recieving
def monitor_Interaction(interaction_id, Federation_ID, existing_policy, pid):
    policy_ID = existing_policy["id"]
    provider_federation = existing_policy.get("providerFederation", {}).get("object")
    # broker_info=fetch_request_by_federation_sender(provider_federation)[0].get("senderAddress")
    # if not broker_info:
    #     broker_info=fetch_response_by_federation_sender(provider_federation)[0].get("senderAddress")
    # if not broker_info:
    #     print(f"[monitor_Interaction] No broker information found for Federation_ID: {Federation_ID}")
    #     return
    # print(f"broker_info: {broker_info}")
    # broker_address, broker_port = broker_info.split(":")
    # broker_port = int(broker_port)
    topic = f"Federation/{provider_federation}/Policy/{policy_ID}"
    print(f"[monitor_Interaction] Monitoring policy updates on topic: {topic}")
    client = mqtt.Client()

    # Set user data with the existing policy and other parameters
    client.user_data_set({
        "existing_policy": existing_policy,
        "interaction_id": interaction_id,
        "Federation_ID": Federation_ID,
        "pid": pid
    })

    def on_message(client, userdata, message):
        # Access the shared data directly from client._userdata
        data = client._userdata
        existing_policy = data["existing_policy"]
        interaction_id = data["interaction_id"]
        Federation_ID = data["Federation_ID"]
        pid = data["pid"]

        new_policy_data = json.loads(message.payload.decode())
        # print(f"[on_message monitoring] Message: {json.dumps(new_policy_data,indent=2)} received on topic {message.topic}")
        # print(f"[on_message] New policy data: {new_policy_data}")

        # Check if the new policy data's lastModified is different
        if new_policy_data["modificationPolicy"]["value"]["lastModified"] != existing_policy["modificationPolicy"]["value"]["lastModified"]:
            # print("[on_message] Policy change detected.")
            data["existing_policy"] = new_policy_data  # Update the policy in userdata

            # Validate the new policy to check if the interaction should continue
            canReceive, permittedContextTypes = validate_Recieving(new_policy_data, Federation_ID.split(":")[-1])
            # print(f"[on_message] Can receive: {canReceive}, Permitted Context Types: {permittedContextTypes}")

            if not canReceive:
                print(f"[on_message] Terminating interaction {interaction_id} due to policy restrictions.")
                terminate_Interaction(interaction_id=interaction_id, pidInput=pid)
                # print(f"terminated time: {time.perf_counter_ns()}")
                os.kill(os.getpid(), signal.SIGTERM)

    client.on_message = on_message
    client.connect(config.FED_BROKER, config.FED_PORT, 60)  
    client.subscribe(topic)
    print(f"[monitor_Interaction] Subscribed to topic: {topic}")
    client.loop_forever()


def query_community_federation(community_id):
    """
    Given a community ID, return the federation to which the community belongs.
    """
    community = Context_Management_Service.get_community_by_id(community_id)
    # print(community)
    
    # Access the partOfFederation field
    part_of_federation_value = community.get("partOfFederation", {}).get("object")
    if community:
        return part_of_federation_value[0]
        
    else:
        return None

def create_Interaction(initiated_By, from_community, towards,Interaction_Type, Interaction_Status,
                    source_data_model, target_data_model,sourcepath,destpath):
    # 1. Create and Store the NGSI-LD Representation
    # Generate a unique identifier for the interaction
    # Spawn a process to monitor the current process (create_Interaction itself)
    # current_process_monitoring = multiprocessing.Process(target=monitor_memory_usage, args=(os.getpid()))
    # current_process_monitoring.start()
    # print(f"startup time: {time.perf_counter_ns()}")
    # startup_time=time.perf_counter_ns()
    # with open("startups.txt", "a") as log_file:
    #                     log_file.write(f"{startup_time}\n")
    #                     log_file.flush()
    
    unique_id = str(uuid.uuid4())[:8]  # Taking the first 8 characters for brevity
    # startup_time=time.perf_counter_ns()
    # Construct the ID for the CommunityInteraction entity
    interaction_id = f"urn:ngsi-ld:CommunityInteraction:{from_community}:{towards}:{unique_id}"
    interaction_entity = {
        "id": interaction_id,
        "type": "CommunityInteraction",
        "initiatedBy": {"type": "Property", "value": f"urn:ngsi-ld:Federation:{initiated_By}"},
        "fromC": {"type": "Property", "value": from_community},
        "towardsC": {"type": "Property", "value": towards},
        "SourceSpecificPath": {"type": "Property", "value": sourcepath},
        "TargetSpecificPath": {"type": "Property", "value": destpath},
        "source_data_model": {"type": "Property", "value": source_data_model},
        "target_data_model": {"type": "Property", "value": target_data_model},
        "connectionType": {"type": "Property", "value": Interaction_Type},
        "connectionStatus": {"type": "Property", "value": Interaction_Status},
        "@context": [context_url, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]
    }
    
    print(f"interaction_id:{interaction_id}")
    # 2. Check if Communities are Part of a Federation
    from_community_federation = query_community_federation(from_community)
    towards_community_federation = query_community_federation(towards)
    if from_community_federation==None or towards_community_federation==None:
        print("Communities should be part of existing federations")
        return
    
    # 3. Validate the Interaction
    validationResult=validate_Interaction(interaction_entity, from_community_federation)
    if isinstance(validationResult, tuple):
        valide, policy = validationResult
    else:
        valide = validationResult
    if not valide:
        print("Interaction validation failed!")
        return None,None
    same_data_model=True
    converter_name=None
    if source_data_model != target_data_model :
        same_data_model=False
        converter_name= find_mapping(source_data_model,target_data_model)
        if converter_name is None:
            return
    
    # print("\n")
    # print("process about to start")
    # Spawn a new process for the interaction based on its type
    process = multiprocessing.Process(target=interaction_process, args=(interaction_id,from_community,towards,Interaction_Type,source_data_model,target_data_model,sourcepath,destpath,same_data_model,converter_name))

    process.start()
    
    monitor_processing=multiprocessing.Process(target=monitor_Interaction, args=(interaction_id,config.FEDERATION_ID,policy,process.pid))
    monitor_processing.start()
    
    # print(f"Interaction {interaction_id} started in a new process with PID {process.pid}")
    # Store the process ID (PID) in the interaction's NGSI-LD data
    interaction_entity["processId"] = {"type": "Property", "value": process.pid}

    try:
        # Step 1: Register the interaction
        response = requests.post(context_broker_url, headers=headers, data=json.dumps(interaction_entity))
        response.raise_for_status() 
        
        interaction_id = interaction_entity['id']
        # print(f"Interaction {interaction_id} Registered Successfully!")

        # Step 2: Fetch the federation entity using federationID derived from initiated_by
        federation_id = interaction_entity["initiatedBy"]["value"]  # Assuming initiated_by is the federationID
        # print(f"\n initiated by: {federation_id}")
        federation_url = f"{context_broker_url}/{federation_id}"
        # print(f"\n Federation URL: {federation_url}")

        federation_entity = Context_Management_Service.get_federation_by_id(federation_id)
        # print(f"\n Federation Entity: {json.dumps(federation_entity,indent=2)}")

        # Step 3: Update the usesConnections attribute with the new interaction ID
        if "usesConnections" not in federation_entity:
            federation_entity["usesConnections"] = {"type": "Relationship", "object": []}
        federation_entity["usesConnections"]["object"].append(interaction_id)
        params = {'type': 'Federation'}
        response = requests.patch(f"{federation_url}/attrs", params=params, headers=headers, json=federation_entity)
        response.raise_for_status()

        # Step 4: Send the updated federation entity back to the context broker
        params = {
            'type': 'Federation',
        }
        patch_url=f"{federation_url}/attrs"
        update_response = requests.patch(patch_url, headers=headers,params=params, data=json.dumps(federation_entity))
        update_response.raise_for_status()
        # print(f"Federation {federation_id} updated successfully with Interaction {interaction_id}")
        return interaction_id,process.pid
    
    except requests.exceptions.RequestException as e:
        print(f"Error registering Interaction: {e}")
        # Print the detailed error response for debugging
        if e.response is not None:
            print(f"Response content: {e.response.content}")
        return None,None
    return interaction_id, process.pid

def get_interaction_by_id(interaction_id):
    
    # print("HERE IS THE INTERACTION ID the original one  " + interaction_id)
    # Remove the prefix if present
    prefix = "urn:ngsi-ld:CommunityInteraction:"
    if interaction_id.startswith(prefix):
        interaction_id = interaction_id[len(prefix):]
    entity_url = f"{context_broker_url}/urn:ngsi-ld:CommunityInteraction:{interaction_id}"

    try:
        # Fetch the context JSON from GitHub
        context_response = requests.get(context_url)
        context_response.raise_for_status()  # Raise an error if fetching fails
        context_json = context_response.json()  # Parse the JSON content

        response = requests.get(entity_url, headers=headersget)
        response.raise_for_status()  # Raise an exception for HTTP errors

        if response.status_code == 200:
            Interaction = response.json()
            # print("HERE IS THE INTERACTION\n")
            # print(json.dumps(Interaction,indent=2))
            return Interaction
        else:
            return None

    except requests.exceptions.RequestException as e:
        print(f"Error retrieving Interaction: {e}")
        return None

def get_interaction_status(interaction_id):
    
    interaction = get_interaction_by_id(interaction_id)
    
    if interaction and 'connectionStatus' in interaction and 'value' in interaction['connectionStatus']:
        return interaction['connectionStatus']['value']
    else:
        return None




def Update_Interaction(interaction_id, connection_Status):
    """
    Pauses, resumes, or activates an interaction status and publishes the update to an MQTT topic in parallel.
    """
    prefix = "urn:ngsi-ld:CommunityInteraction:"
    if interaction_id.startswith(prefix):
        interaction_id = interaction_id[len(prefix):]
    entity_url = f"{context_broker_url}/urn:ngsi-ld:CommunityInteraction:{interaction_id}"
    Interaction = get_interaction_by_id(interaction_id)
    
    update_payload = {
        "id": Interaction["id"],
        "type": Interaction["type"],
        "@context": Interaction["@context"],
        "connectionStatus": {"type": "Property", "value": connection_Status}
    }

    # Function to update status in context broker
    def update_context_broker():
        params = {'type': 'CommunityInteraction'}
        patch_url = f"{entity_url}/attrs"
        response = requests.patch(
            patch_url, params=params, headers=headers, data=json.dumps(update_payload)
        )
        response.raise_for_status()
        if response.status_code == 204:
            print(f"Context Broker update successful for interaction {interaction_id} with status {connection_Status}.")
        else:
            print(f"Context Broker update failed for interaction {interaction_id}.")

    # Function to publish the status update on the MQTT topic
    def publish_to_mqtt():
        mqtt_client = mqtt.Client()
        mqtt_client.connect(config.FED_BROKER, config.FED_PORT, 60)
        topic = f"interaction/status/{prefix}{interaction_id}"
        mqtt_client.publish(topic, connection_Status)
        # print(f"Published {connection_Status} status to MQTT topic {topic}")
        mqtt_client.disconnect()

    # Run both functions in parallel
    context_broker_thread = threading.Thread(target=update_context_broker)
    mqtt_thread = threading.Thread(target=publish_to_mqtt)

    # Start both threads
    context_broker_thread.start()
    mqtt_thread.start()

    # Wait for both threads to complete
    context_broker_thread.join()
    mqtt_thread.join()

    print(f"Interaction {interaction_id} status update completed in both Context Broker and MQTT.")




def terminate_Interaction(interaction_id,pidInput=None):
    # Retrieve the interaction's data from the database
    interaction_data = get_interaction_by_id(interaction_id)
    
    pid = interaction_data.get("processId", {}).get("value")

    # This will return `None` if "processId" or "value" is missing, so you can handle it accordingly
    if pid is None and pidInput is not None:
        pid=pidInput
        
    
    # Terminate the process
    try:
        os.kill(pid, signal.SIGTERM)  # Sends the SIGTERM signal to the process
        # terminated_time=time.perf_counter_ns()
        print("hello my policy update time is" + str(policy_update_time/1000000) + "and my terminated time is" + str(terminated_time/1000000))   
        # delay=(terminated_time-policy_update_time)/1_000_000
        # print(f"delay:{delay}")
        # # print(f"terminated time: {terminated_time}")
        # # threading.Thread(target=log_time, args=(delay,)).start()
        # with open("delay.txt", "a") as log_file:
        #     log_file.write(f"{delay}\n")
        #     log_file.flush()
        remove_Interaction(interaction_id)
        return True 
    except ProcessLookupError:
        print(f"No process with PID {pid} found.")
        return True  
    except PermissionError:
        print(f"Permission denied to terminate PID {pid}.") 
        return False  

def list_Interactions():
    """
    Retrieves a list of all interactions.
    """
    return Context_Management_Service.get_list("CommunityInteraction")

def remove_Interaction(interaction_id):
    """
    Deletes an interaction based on its unique identifier.
    
    Logic:
    - Find and delete the interaction by ID
    """
    
    # if not terminate_Interaction(interaction_id):
    #     print(f"Failed to terminate the process for interaction {interaction_id}")
    #     return None   
    prefix = "urn:ngsi-ld:CommunityInteraction:"
    if interaction_id.startswith(prefix):
        interaction_id = interaction_id[len(prefix):]
    entity_url = f"{context_broker_url}/urn:ngsi-ld:CommunityInteraction:{interaction_id}"

    try:
        interaction_entity=get_interaction_by_id(interaction_id)
        # print(interaction_entity)
        federation_id = interaction_entity["initiatedBy"]["value"]  # Assuming initiated_by is the federationID
        # print(federation_id)
        federation_entity =Context_Management_Service.get_federation_by_id(federation_id)
        # print(federation_entity)
        if federation_entity is None:
            print(f"Federation with ID '{federation_id}' not found.")
            return None
        if 'usesConnections' in federation_entity:
            # print(federation_entity['usesConnections'])
            try:
                federation_url=f"{context_broker_url}/{federation_id}"
                federation_entity["usesConnections"]["object"].remove(f"urn:ngsi-ld:CommunityInteraction:{interaction_id}")
                params = {'type': 'Federation'}
                response = requests.patch(f"{federation_url}/attrs",params=params, headers=headers, json=federation_entity)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Error removing Interaction from the Federation: {e}")
                return None 
        delete_response = requests.delete(entity_url, headers=headers)
        delete_response.raise_for_status()  # Raise an exception for HTTP errors (e.g., 404 Not Found)

        if delete_response.status_code == 204:  # 204 No Content indicates successful deletion
            print(f"Interaction {interaction_id} Removed Successfully!")
        else:
            print(f"Unexpected response code: {delete_response.status_code}")
        
    except requests.exceptions.RequestException as e:
        print(f"Error deleting Interaction: {e}")

# if __name__ == "__main__":
#     interaction_id, pid = create_Interaction(
#             "Federation2", "Community1", "Community2", "community", "active",
#             "Brick", "NGSI-LD", "community1/occupancy","" 
#         )
#     print(f"Interaction created with ID: {interaction_id} and PID: {pid}")
