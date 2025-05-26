import paho.mqtt.client as mqtt
import json
from multiprocessing import Process
from uuid import uuid4
from datetime import datetime, timezone
import requests
import config as config
import time

# MQTT and Context Broker configurations from config.py
FED_BROKER = config.FED_BROKER
FED_PORT = config.FED_PORT
CONTEXT_BROKER_URL = config.CONTEXT_BROKER_URL
FEDERATION_ID = config.FEDERATION_ID
context_url="https://raw.githubusercontent.com/NiematKhoder/test/main/Context.json"
headers = {'Content-Type': 'application/ld+json'}
link_header_value = f'<{json.dumps(context_url).replace(" ", "")}>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
headersget = {
            "Accept": "application/ld+json",  # Request JSON-LD format
            "Link": f'<{context_url}>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
        }

def get_list(type, context_broker_url, federation_id, limit=None, headers=headersget):
    """
    Retrieves a list of communities that are part of a specified federation from the context broker.

    Args:
        type (str): The type of entities to query, in this case "Community".
        context_broker_url (str): The URL of the context broker.
        federation_id (str): The ID of the federation to filter communities by.
        limit (int, optional): The maximum number of communities to retrieve. Defaults to None, meaning no limit.

    Returns:
        list: A list of dictionaries representing the communities, or None if an error occurs.
    """
    federation_id=f"urn:ngsi-ld:Federation:{federation_id}"
    # Parameters for retrieving entities by type
    # params = {
    #     "type": type  # Query only for entities of type "Community"
    # }
    
    # Apply limit if specified
    if limit is not None:
        params["limit"] = limit

    try:
        # Send GET request to retrieve all communities of specified type
        response = requests.get(f"{context_broker_url}/?type={type}", headers=headersget)
        response.raise_for_status()
        
        if response.status_code == 200:
            communities = response.json()
            # print(communities)
            # Filter to only include communities where the federation ID is within partOfFederation.object array
            filtered_communities = [
                community for community in communities
                if "partOfFederation" in community 
                and federation_id in community["partOfFederation"]["object"]
            ]
            return filtered_communities
        else:
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving Communities: {e}")
        return None

def get_data_models(context_broker_url, selected_community, limit=None, headers=headersget):
    """
    Retrieves a list of data models associated with any of the specified communities.

    Args:
        context_broker_url (str): The URL of the context broker.
        selected_community (list): A list of community IDs to filter data models by.
        limit (int, optional): The maximum number of data models to retrieve. Defaults to None, meaning no limit.

    Returns:
        list: A list of dictionaries representing the data models, or None if an error occurs.
    """

    # Parameters for retrieving entities by type
    params = {
        "type": "DataModel"  # Query only for entities of type "DataModel"
    }
    
    # Apply limit if specified
    if limit is not None:
        params["limit"] = limit

    try:
        # Send GET request to retrieve all data models of specified type
        response = requests.get(context_broker_url, headers=headers, params=params)
        response.raise_for_status()
        
        if response.status_code == 200:
            data_models = response.json()

            # Filter to only include data models with one of the selected communities in associated_Communities.object
            filtered_data_models = [
                data_model for data_model in data_models
                if "associated_Communities" in data_model
                and any(community in data_model["associated_Communities"].get("object", []) for community in selected_community)
            ]
            return filtered_data_models
        else:
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving DataModels: {e}")
        return None

def remove_extra_slashes(url):
    return url.replace("//", "/")

# def check_other_federation(policy, input_federation_id):
#     # Access the sharing rules and Context Broker URL from the policy
#     # if isinstance(policy,str):
#     #     policy=json.loads(policy)
#     sharing_rules = policy.get("sharingRules", {}).get("value", [])
    
#     context_broker_url = str(policy.get("ContextBrokerURL", {}).get("value")).replace("//", "/")
#     # print(context_broker_url1)
#     # context_broker_url=remove_extra_slashes(f"{context_broker_url1}")
#     print(context_broker_url)
#     # Initialize a list to collect federation IDs that are different from input_federation_id
#     other_federation_ids = []
    
#     # Loop through each federation in the sharing rules
#     for federation_rule in sharing_rules:
#         if isinstance(federation_rule,str):
#             federation_rule=json.loads(federation_rule)
#         # Get the federation ID and permissions
#         for federation_id in federation_rule.keys():
#             # Check if the federation ID is different from the input federation ID
#             if federation_id != input_federation_id:
#                 federation_id=f"urn:ngsi-ld:Federation:{federation_id}"
#                 other_federation_ids.append(federation_id)
    
#     # Return the list of other federation IDs and Context Broker URL
#     return other_federation_ids, context_broker_url

def check_other_federation(policy, input_federation_id):
    """
    Identifies federations other than the input federation ID based on the sharing rules in the policy.
    
    Args:
        policy (dict): The policy dictionary containing sharing rules and other information.
        input_federation_id (str): The federation ID being processed.

    Returns:
        tuple: A list of other federation IDs and the Context Broker URL, or None if no other federations are found.
    """
    sharing_rules = policy.get("sharingRules", {}).get("value", [])
    context_broker_url = str(policy.get("ContextBrokerURL", {}).get("value", "")).replace("//", "/")
    print("Context Broker URL:", context_broker_url)

    other_federation_ids = []

    for federation_rule in sharing_rules:
        print(f"Raw federation_rule: {federation_rule}")
        if not federation_rule:
            print("Skipping invalid federation_rule:", federation_rule)
            continue  # Skip invalid entries

        if isinstance(federation_rule, str):
            try:
                federation_rule = json.loads(federation_rule)
            except json.JSONDecodeError as e:
                print(f"Error decoding federation_rule: {e}")
                continue  # Skip invalid JSON

        for federation_id in federation_rule.keys():
            if federation_id != input_federation_id:
                federation_id = f"urn:ngsi-ld:Federation:{federation_id}"
                other_federation_ids.append(federation_id)

    # If no other federations are found, return None
    if not other_federation_ids:
        print("No other federations found.")
        return [],""

    return other_federation_ids, context_broker_url

def validate_forwarding(policy, federationID1, federationID2):
    # Access the sharing rules and permitted context types from the policy
    sharing_rules = policy.get("sharingRules", {}).get("value", [])
    permitted_context_types = policy.get("permittedContextTypes", {}).get("value", [])
    
    # Flags for the federation conditions
    federation1_can_forward = False
    federation2_canreceive = True
    canForward=False
    # Loop through each federation in the sharing rules
    for federation_rule in sharing_rules:
        for federation_id, permissions in federation_rule.items():
            if federation_id == federationID1:
                # Check if federationID1 has canForward permission
                if str(permissions["canForward"]).lower()=="true":
                    federation1_can_forward = True
            elif federation_id == federationID2:
                # Mark federation2 as existing in the sharing rules
                if str(permissions["canReceive"]).lower()=="false":
                    federation2_canreceive = False
            elif str(federation_id).lower()=="public":
                if str(permissions["canReceive"]).lower()=="true" and str(permissions["canForward"]).lower()=="true":
                    federation1_can_forward=True
                
    # Return True with permittedContextTypes if federation1 can forward and federation2 can receive
    if federation1_can_forward and federation2_canreceive :
        return True, permitted_context_types
    else:
        return False


def fetch_all_policies(context_broker):
    """
    Fetch all policies from the context broker.
    """
    try:
        response = requests.get(f"{context_broker}?type=ContextPolicy", headers={'Content-Type': 'application/json'})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching policies: {e}")
        return []
    
def fetch_policy_by_provider_federation(provider_federation,context_broker):
    """
    Fetch the policy based on the provider federation.
    """
    provider_federation = f"urn:ngsi-ld:Federation:{provider_federation}"
    all_policies = fetch_all_policies(context_broker)
    for policy in all_policies:
        if policy.get("providerFederation", {}).get("object") == provider_federation:
            # print(policy)
            return policy
    return None
# policy=fetch_policy_by_provider_federation("Federation2")
# print(json.dumps(policy,indent=2))
# print("---------------------------------------------------")

def get_federation_by_id(federation_Id, context_broker_url):

    entity_url = f"{context_broker_url}/urn:ngsi-ld:Federation:{federation_Id}"

    try:
        response = requests.get(entity_url, headers=headersget)
        response.raise_for_status()  # Raise an exception for HTTP errors

        if response.status_code == 200:
            federation = response.json()
            return federation
        else:
            return None

    except requests.exceptions.RequestException as e:
        print(f"Error retrieving Federation: {e}")
        return None        
# federation=get_federation_by_id("Federation2","http://localhost:1027/ngsi-ld/v1/entities")
# print(json.dumps(federation,indent=2))
# print("---------------------------------------------------")
# def get_list(type,context_broker_url, limit=None,headers=headersget):
#     """
#     Retrieves a list of federations from the context broker.

#     Args:
#         limit (int, optional): The maximum number of federations to retrieve.
#                         If not provided, all federations will be returned.

#     Returns:
#         list: A list of dictionaries representing the federations, or None if an error occurs.
#     """

#     params = {
#         "type": type  # Query only for entities of type "Federation"
#     }
    
#     if limit is not None:
#         params["limit"] = limit  # Add the limit parameter to the query

#     try:
#         response = requests.get(context_broker_url, headers=headersget, params=params)
#         response.raise_for_status()
#         if response.status_code == 200:
#             federations = response.json()
#             return federations
#         else:
#             return None
#     except requests.exceptions.RequestException as e:
#         print(f"Error retrieving Federations: {e}")
#         return None

def store_context(entity):
    if entity is None:
        print("entity is None, skipping storage.")
        return
    if isinstance(entity,str):
        entity=json.loads(entity)
    headers = {'Content-Type': 'application/ld+json'}
    entity_id = entity['id']
    try:
        # Check if entity already exists
        response = requests.get(f"{CONTEXT_BROKER_URL}/{entity_id}")
        if response.status_code == 200:
            # entity exists, delete it
            response = requests.delete(f"{CONTEXT_BROKER_URL}/{entity_id}")
            response.raise_for_status()
            print(f"entity {entity_id} deleted successfully from {CONTEXT_BROKER_URL}")

        # Post the new policy
        response = requests.post(CONTEXT_BROKER_URL, json=entity, headers=headers)
        response.raise_for_status()
        print(f"entity {entity_id} stored successfully in {CONTEXT_BROKER_URL}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to store entity {entity_id}:", e)



# communities=get_list("Community","http://localhost:1027/ngsi-ld/v1/entities")
# print(json.dumps(communities,indent=2))
def store_Federation_Context_based_policy(Federation_ID):
    """
    Store context of the policy provider in the context broker.
    """
    policy=fetch_policy_by_provider_federation(Federation_ID,config.CONTEXT_BROKER_URL)
    # policy=json.dumps(policy)
    # print(policy)
    # context_broker_url=policy["ContextBrokerURL"]["value"]
        # Safely access the ContextBrokerURL value
    if "ContextBrokerURL" in policy and policy["ContextBrokerURL"]["value"] is not None:
        context_broker_url = policy["ContextBrokerURL"].get("value", "").replace("//", "/")
        if not context_broker_url:
            print("ContextBrokerURL value is empty.")
    else:
        print("ContextBrokerURL is missing or None.")
        context_broker_url = None
    
    # Output the result for debugging
    print(f"Context Broker URL: {context_broker_url}")
    
    permitted_types = policy.get("permittedContextTypes", {}).get("value", [])
    print(permitted_types)
    if "federations" or "federation"  in permitted_types:
        federation=get_federation_by_id(Federation_ID,context_broker_url)
        store_context(federation)
    if "communities".lower() or "community".lower()  in permitted_types:
        # print("communities")
        communities=get_list("Community",context_broker_url,Federation_ID)
        print(communities)
        communities_ids=[]
        for community in communities:
            print("for community in communities:")
            communities_ids.append(community["id"])
            store_context(community)
        if "datamodels" or "datamodel"  in permitted_types:
            datamodels=get_data_models(context_broker_url,communities_ids)
            for datamodel in datamodels:
                store_context(datamodel)
    if "datamodels" or "datamodel"  in permitted_types:
        communities=get_list("Community",context_broker_url,Federation_ID)
        communities_ids=[]
        for community in communities:
            communities_ids.append(community["id"])
        datamodels=get_data_models(context_broker_url,communities_ids)
        for datamodel in datamodels:
            store_context(datamodel)   
    other_federations=check_other_federation(policy,Federation_ID)[0]
    if not other_federations:
        for other_federation in other_federations:
            other_policy=fetch_policy_by_provider_federation(other_federation,context_broker_url)
            result=validate_forwarding(other_policy,Federation_ID,FEDERATION_ID.split(":")[-1])
            if result[0]==True:
                if "federations" or "federation"  in result[1]:
                    federation=get_federation_by_id(Federation_ID,context_broker_url)
                    store_context(federation)
                if "communities" or "community"  in result[1]:
                    communities=get_list("Community",context_broker_url,other_federation.split(":")[-1])
                    communities_ids=[]
                    for community in communities:
                        communities_ids.append(community["id"])
                        store_context(community)
                    if "datamodels" or "datamodel"  in result[1]:
                        datamodels=get_data_models(context_broker_url,communities_ids)
                        for datamodel in datamodels:
                            store_context(datamodel)
                if "datamodels" or "datamodel"  in result[1]:
                    communities=get_list("Community",context_broker_url,other_federation.split(":")[-1])
                    communities_ids=[]
                    for community in communities:
                        communities_ids.append(community["id"])
                    datamodels=get_data_models(context_broker_url,communities_ids)
                    for datamodel in datamodels:
                        store_context(datamodel)
    
# store_Federation_Context_based_policy("Federation2")