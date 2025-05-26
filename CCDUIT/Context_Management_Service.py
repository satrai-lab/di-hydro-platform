import requests
import json 
import config

context_broker_url = config.CONTEXT_BROKER_URL  # Replace with your context broker's URL
context_url="https://raw.githubusercontent.com/NiematKhoder/test/main/Context.json"

headers = {'Content-Type': 'application/ld+json'}
link_header_value = f'<{json.dumps(context_url).replace(" ", "")}>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
headersget = {
            "Accept": "application/ld+json",  # Request JSON-LD format
            "Link": f'<{context_url}>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
        }

#list based on the type
def get_list(type, limit=None,headers=headersget):
    """
    Retrieves a list of federations from the context broker.

    Args:
        limit (int, optional): The maximum number of federations to retrieve.
                        If not provided, all federations will be returned.

    Returns:
        list: A list of dictionaries representing the federations, or None if an error occurs.
    """

    params = {
        "type": type  # Query only for entities of type "Federation"
    }
    
    if limit is not None:
        params["limit"] = limit  # Add the limit parameter to the query

    try:
        response = requests.get(context_broker_url, headers=headersget, params=params)
        response.raise_for_status()
        if response.status_code == 200:
            federations = response.json()
            return federations
        else:
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving Federations: {e}")
        return None

def handle_registration_error(e):
    """Provides consistent error handling for registration attempts."""
    print(f"Error registering Community Model: {e}")
    if e.response is not None:
        print(f"Response content: {e.response.content}")

# ------------------------------------------------------------------------------
# Federations Management 
# ------------------------------------------------------------------------------
#Regsiter Federation
def register_Federation(federation_Id, name, topology, structure, areaCovered, number_Of_Nodes,
                        part_Of_Federation=None, includes_Communities=None, uses_Interactions=None):

    existing_entity_url = f"{context_broker_url}/urn:ngsi-ld:Federation:{federation_Id}"
    try:
        response = requests.get(existing_entity_url, headers={'Accept': 'application/ld+json'})
        print(response.json)
        response.raise_for_status()
        if response.status_code == 200:
            print(f"Federation {federation_Id} already exists!")
            return
    except requests.exceptions.RequestException:
        pass 

    # Construct the NGSI-LD entity, ensuring 'object' is present for relationships
    entity = {
        "id":  f"urn:ngsi-ld:Federation:{federation_Id}",
        "type": "Federation",
        "name": {"type": "Property", "value": name},
        "topology": {"type": "Property", "value": topology},
        "structure": {"type": "Property", "value": structure},
        "areaCovered": {"type": "Property", "value": areaCovered},
        "numberOfNodes": {"type": "Property", "value": number_Of_Nodes},
        "partOfFederation": {"type": "Relationship", "object": []},   # Empty list as object
        "includesCommunities": {"type": "Relationship", "object": []}, # Empty list as object
        "usesConnections": {"type": "Relationship", "object": []},    # Empty list as object
        "@context": [context_url, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]
    }

    # Add values to relationships only if provided
    if part_Of_Federation:
            if not part_Of_Federation.startswith("urn:ngsi-ld:Federation:"):
                part_Of_Federation1 = f"urn:ngsi-ld:Federation:{part_Of_Federation}"
            entity["partOfFederation"]["object"] = part_Of_Federation1
    if includes_Communities:
        if isinstance(includes_Communities, str):  # If it's a single string, convert to a list
            includes_Communities = [includes_Communities]
        entity["includesCommunities"]["object"] = [f"urn:ngsi-ld:Community:{community}" for community in includes_Communities]
        #here I want to add the federation to the entity.
    if uses_Interactions:
        if isinstance(uses_Interactions, str):  # If it's a single string, convert to a list
            uses_Interactions = [uses_Interactions]
        entity["usesConnections"]["object"] = [f"urn:ngsi-ld:Interaction:{interaction}" for interaction in includes_Communities]

    try:
        response = requests.post(context_broker_url, headers=headers, data=json.dumps(entity))
        response.raise_for_status()
        print(f"Federation {federation_Id} Registered Successfully!")

    except requests.exceptions.RequestException as e:
        print(f"Error registering Federation: {e}")
        if e.response is not None:
            print(f"Response content: {e.response.content}")

# federation_Id = "e4c_103"  # Unique identifier for your federation
# name = "Smart Campus"
# topology = "Mesh"          # Example topology (star, mesh, hybrid, etc.)
# structure = "Federated"       # Example structure (centralized, decentralized, etc.)
# areaCovered = "ile de france"
# number_Of_Nodes = 100
# # includes_Communities="communitytest"
# register_Federation(federation_Id, name, topology, structure, areaCovered, number_Of_Nodes)

#Get Ferderation
def get_federation_by_id(federation_Id):

    prefix = "urn:ngsi-ld:Federation:"
    if federation_Id.startswith(prefix):
        federation_Id = federation_Id[len(prefix):]
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

# federation_data =json.dumps(get_federation_by_id("e4c_103"),indent=2)
# print(federation_data)    

#delete federation
def delete_federation_by_id(federation_Id):
    """
    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    
    entity_url = f"{context_broker_url}/urn:ngsi-ld:Federation:{federation_Id}"

    try:
        response = requests.delete(entity_url)
        response.raise_for_status()  # Raise an exception for HTTP errors (e.g., 404 Not Found)

        if response.status_code == 204:  # 204 No Content indicates successful deletion
            print(f"Federation with ID '{federation_Id}' deleted successfully.")
            return True
        else:
            print(f"Unexpected response code: {response.status_code}")
            return False
    
    except requests.exceptions.RequestException as e:
        print(f"Error deleting Data Model: {e}")
        return False
    
#update federation
def update_federation(federation_Id, name=None, topology=None, structure=None, areaCovered=None, number_Of_Nodes=None):

    entity_url = f"{context_broker_url}/urn:ngsi-ld:Federation:{federation_Id}"

    try:
        # Fetch existing Federation
        existing_federation = get_federation_by_id(federation_Id)
        if existing_federation is None:
            print(f"Federation with ID '{federation_Id}' not found.")
            return False

        update_payload = {
            "id": existing_federation["id"],
            "type": existing_federation["type"],
            "@context": existing_federation["@context"]  
        }

        # Update Properties
        for prop, value in [
            ("name", name),
            ("topology", topology),
            ("structure", structure),
            ("areaCovered", areaCovered),
            ("numberOfNodes", number_Of_Nodes),
        ]:
            if value is not None:
                update_payload[prop] = {"type": "Property", "value": value}
        # Send PATCH request
        params = {
            'type': 'Federation',
        }
        patch_url=f"{entity_url}/attrs"
        response = requests.patch(patch_url,params=params, headers=headers, json=update_payload)
        response.raise_for_status()
        if response.status_code == 204:
            print(f"Federation with ID '{federation_Id}' updated successfully.")
            return True
        else:
            print(f"Unexpected response code: {response.status_code}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"Error updating Federation: {e}")
        return False
# update_federation("e4c_103",name="E4C Hub")
# print(json.dumps(get_federation_by_id("e4c_103"),indent=2))
def get_community_by_id(community_Id):

    entity_url = f"{context_broker_url}/urn:ngsi-ld:Community:{community_Id}"

    try:
        response = requests.get(entity_url, headers=headersget)
        response.raise_for_status()  # Raise an exception for HTTP errors

        if response.status_code == 200:
            federation = response.json()
            return federation
        else:
            return None

    except requests.exceptions.RequestException as e:
        print(f"Error retrieving Data Model: {e}")
        return None        
# print(json.dumps(get_community_by_id("Community1"),indent=2))

def remove_federation_community_rel(federation_Id, community_ID):
    federation_urn=f"urn:ngsi-ld:Federation:{federation_Id}"
    federation_url = f"{context_broker_url}/{federation_urn}"
    community_urn=f"urn:ngsi-ld:Community:{community_ID}"
    community_url= f"{context_broker_url}/{community_urn}"
    try:
        # Fetch existing Federation
        existing_federation = get_federation_by_id(federation_Id)
        existing_community=get_community_by_id(community_ID)
        if existing_federation and existing_community is None:
            print(f"Federation with ID '{federation_Id}'  or community with ID '{community_ID}' not found.")
            return False
        try:
            existing_federation["includesCommunities"]["object"].remove(community_urn)
            params = {'type': 'Federation'}
            response = requests.patch(f"{federation_url}/attrs",params=params, headers=headers, json=existing_federation)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error removing old federation from the community: {e}")
            return False
        try:
            existing_community["partOfFederation"]["object"].remove(federation_urn)
            params = {'type': 'Community'}
            response = requests.patch(f"{community_url}/attrs",params=params, headers=headers, json=existing_community)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error removing old federation from the community: {e}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"Error deleting community from Federation: {e}")
        return False  
    return True

# print(remove_federation_community_rel("e4c_103","Building103"))
# ------------------------------------------------------------------------------
# Communities Management 
# ------------------------------------------------------------------------------
#Regsiter community
def register_Community(community_Id, name, connection_Details, origin, role_In_Federation,
                    geographical_Location, last_Updated, has_Data_Models=None, part_Of_Federation=None):

    existing_entity_url = f"{context_broker_url}/urn:ngsi-ld:Community:{community_Id}"
    community_urn = f"urn:ngsi-ld:Community:{community_Id}" 
    try:
        response = requests.get(existing_entity_url, headers={'Accept': 'application/ld+json'})
        response.raise_for_status()
        if response.status_code == 200:
            print(f"Community {community_Id} already exists!")
            return False
    except requests.exceptions.RequestException:
        pass

    entity = {
        "id": f"urn:ngsi-ld:Community:{community_Id}",
        "type": "Community",
        "name":{"type": "Property", "value":name},
        "connectionDetails": {"type": "Property", "value": connection_Details},
        "origin": {"type": "Property", "value": origin},
        "roleInFederation": {"type": "Property", "value": role_In_Federation},
        "geographicalLocation": {"type": "Property", "value": geographical_Location},
        "hasDataModels": {"type": "Relationship", "object": []},
        "partOfFederation": {"type": "Relationship", "object": []},
        "lastUpdated": {"type": "Property", "value": last_Updated},
        "@context": [context_url, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]
    }

    if has_Data_Models:
        if isinstance(has_Data_Models, str):
            has_Data_Models = [has_Data_Models]
        entity["hasDataModels"]["object"] = [f"urn:ngsi-ld:DataModel:{model}" for model in has_Data_Models]

    if part_Of_Federation:
        if get_federation_by_id(part_Of_Federation):
            if not part_Of_Federation.startswith("urn:ngsi-ld:Federation:"):
                part_Of_Federation1 = f"urn:ngsi-ld:Federation:{part_Of_Federation}"
            entity["partOfFederation"]["object"] = [part_Of_Federation1]
        else:
            print(f"Federation with ID '{part_Of_Federation}' not found. Cannot add relationship.")

    try:
        response = requests.post(context_broker_url, headers=headers, data=json.dumps(entity))
        response.raise_for_status()
        print(f"Community Model {community_Id} Registered Successfully!")
    except requests.exceptions.RequestException as e:
        handle_registration_error(e)
        return False

    if part_Of_Federation:
        if get_federation_by_id(part_Of_Federation):  
            if not part_Of_Federation.startswith("urn:ngsi-ld:Federation:"):
                part_Of_Federation = f"urn:ngsi-ld:Federation:{part_Of_Federation}"
            try:
                response = requests.get(f"{context_broker_url}/{part_Of_Federation}", headers=headersget)
                response.raise_for_status()
                federation = response.json()

                if community_urn not in federation["includesCommunities"]["object"]:
                    federation["includesCommunities"]["object"].append(community_urn)
                    params = {
                        'type': 'Federation',
                    }
                    response = requests.patch(f"{context_broker_url}/{part_Of_Federation}/attrs",params=params, headers=headers,
                                            json=federation)
                    response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Error updating federation: {e}")
                return False

    if has_Data_Models:
        for model_id in has_Data_Models:
            if not model_id.startswith("urn:ngsi-ld:DataModel:"):
                model_id = f"urn:ngsi-ld:DataModel:{model_id}"
            try:
                response = requests.get(f"{context_broker_url}/{model_id}", headers=headersget)
                response.raise_for_status()
                data_model = response.json()

                if community_urn not in data_model["associated_Communities"]["object"]:
                    data_model["associated_Communities"]["object"].append(community_urn)
                    params = {
                        'type': 'DataModel',
                    }
                    response = requests.patch(f"{context_broker_url}/{model_id}/attrs",params=params, headers=headers, json=data_model)
                    response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Error updating data model '{model_id}': {e}")
                return False

    get_response = requests.get(existing_entity_url, headers = headersget)
    get_response.raise_for_status()
    print("Retrieved Data Model:\n", json.dumps(get_response.json(), indent=2))

    return True 


# success = register_Community("Community1","Community1",
#                             {
#                                 "endpoint":"localhost:1888",
#                                 "protocol":"MQTT"
#                             },
#                             "France","Occupancy Data Provider",
#                             "","2024-07-08T12:30:00Z",
#                             has_Data_Models="datamodel1",
#                             part_Of_Federation="Federation1")  # Unpack dictionary as arguments
# if success:
#     print("Community registered and federation updated successfully!")
# else:
#     print("Community registration or federation update failed. Check logs for details.")
# print("------------------------------------------------------------------")    
# print(json.dumps(get("Community"),indent=2))
# print("------------------------------------------------------------------")
# print(json.dumps(get_list("Federation"),indent=2))
# print("------------------------------------------------------------------")
# print(json.dumps(get_list("DataModel"),indent=2))



# def update_community(community_Id, name=None, connection_Details=None, origin=None,
#                     role_In_Federation=None, geographical_Location=None,
#                     has_Data_Models=None, part_Of_Federation=None, last_Updated=None):
    
#     community_urn = f"urn:ngsi-ld:Community:{community_Id}"
#     entity_url = f"{context_broker_url}/{community_urn}"
#     try:
#         response = requests.get(entity_url, headers=headersget)
#         response.raise_for_status()
#         existing_community = response.json()
        
#         print(json.dumps(existing_community,indent=2))
#     except requests.exceptions.RequestException as e:
#         print(f"Error retrieving community: {e}")
#         return False

#     update_payload = {} 
#     for attr, value in [
#         ("name", name),
#         ("connectionDetails", connection_Details),
#         ("origin", origin),
#         ("roleInFederation", role_In_Federation),
#         ("geographicalLocation", geographical_Location),
#         ("lastUpdated", last_Updated)
#     ]:
#         if value is not None:
#             update_payload[attr] = {"type": "Property", "value": value}
#     for rel, value in [
#         ("hasDataModels", has_Data_Models),
#         ("partOfFederation", part_Of_Federation)
#     ]:
#         if value is not None:
#             print(value)
#             if isinstance(value, str):
#                 value = [value]
#             if rel == "hasDataModels":
#                 update_payload[rel] = {"type": "Relationship", "object": [f"urn:ngsi-ld:DataModel:{model}" for model in value]}  
#                 existing_data_models = existing_community.get("hasDataModels", {}).get("object", [])
#                 models_to_remove = [model for model in existing_data_models if model not in value]
#                 print(models_to_remove)
                
#                 for model_urn in models_to_remove:
#                     model_url = f"{context_broker_url}/{model_urn}"
#                     try:
#                         existing_community["hasDataModels"]["object"].remove(model_urn)
#                         params = {'type': 'Community'}
#                         response = requests.patch(f"{entity_url}/attrs",params=params, headers=headers, json=existing_community)
#                         response.raise_for_status()
#                     except requests.exceptions.RequestException as e:
#                         print(f"Error removing old model from the community: {e}")
#                         return False
#                     try:
#                         response = requests.get(model_url, headers=headersget)
#                         response.raise_for_status()
#                         data_model = response.json()
#                         data_model["associated_Communities"]["object"].remove(community_urn)
#                         print(data_model)
#                         params = {'type': 'DataModel'}
#                         response = requests.patch(f"{model_url}/attrs", params=params, headers=headers, json=data_model)
#                         response.raise_for_status()
#                     except requests.exceptions.RequestException as e:
#                         print(f"Error updating data model '{model_urn}': {e}")
#                         return False

#                 for model_urn in value: 
#                     model_urn=f"urn:ngsi-ld:DataModel:{model_urn}"
#                     model_url = f"{context_broker_url}/{model_urn}"
#                     try:
#                         response = requests.get(model_url, headers=headersget)
#                         response.raise_for_status()
#                         data_model = response.json()

#                         if community_urn not in data_model.get("associated_Communities", {}).get("object", []):
#                             if "associated_Communities" not in data_model:
#                                 data_model["associated_Communities"] = {"type": "Relationship", "object": []}
#                             data_model["associated_Communities"]["object"].append(community_urn)
#                             params = {'type': 'DataModel'}
#                             response = requests.patch(f"{model_url}/attrs",params=params, headers=headers, json=data_model)
#                             response.raise_for_status()
#                     except requests.exceptions.RequestException as e:
#                         print(f"Error updating data model '{model_urn}': {e}")
#                         return False
            
#             if rel == "partOfFederation":
#                 print("partOfFederation")
#                 update_payload[rel] = {"type": "Relationship", "object": [f"urn:ngsi-ld:Federation:{fed}" for fed in value]}   
#                 # print(json.dumps(update_payload,indent=2))
#                 if "partOfFederation" in existing_community and existing_community["partOfFederation"]["object"]:
#                     old_federation_urn = existing_community["partOfFederation"]["object"][0]
#                     # print(f"old_federation_urn:{old_federation_urn}")
                    
#                     old_federation_url = f"{context_broker_url}/{old_federation_urn}"
#                     try:
#                         existing_community["partOfFederation"]["object"].remove(old_federation_urn)
#                         params = {'type': 'Community'}
#                         response = requests.patch(f"{entity_url}/attrs",params=params, headers=headers, json=existing_community)
#                         response.raise_for_status()
#                     except requests.exceptions.RequestException as e:
#                         print(f"Error removing old federation from the community: {e}")
#                         return False
                    
#                     try:
#                         response = requests.get(old_federation_url, headers=headersget)
#                         response.raise_for_status()
#                         old_federation = response.json()
#                         old_federation["includesCommunities"]["object"].remove(community_urn)
#                         params = {'type': 'Federation'}
#                         response = requests.patch(f"{old_federation_url}/attrs",params=params, headers=headers, json=old_federation)
#                         response.raise_for_status()
#                     except requests.exceptions.RequestException as e:
#                         print(f"Error updating old federation: {e}")
#                         return False

#                 if value:
#                     new_federation_urn = value[0]
#                     print(new_federation_urn)
#                     new_federation_url = f"{context_broker_url}/urn:ngsi-ld:Federation:{new_federation_urn}"
#                     try:
#                         response = requests.get(new_federation_url, headers=headersget)
#                         response.raise_for_status()
#                         new_federation = response.json()
#                         # print(new_federation)
#                         new_federation["includesCommunities"]["object"].append(community_urn)
#                         # print(f"new_federation after add {community_urn} : {json.dumps(new_federation,indent=2)}")
#                         params = {'type': 'Federation'}
#                         response = requests.patch(f"{new_federation_url}/attrs",params=params, headers=headers, json=new_federation)
#                         response.raise_for_status()
#                     except requests.exceptions.RequestException as e:
#                         print(f"Error updating new federation: {e}")
#                         return False

#     try:
#         print(json.dumps(update_payload,indent=2))
#         params = {'type': 'Community'}
#         response = requests.patch(f"{context_broker_url}/{community_urn}/attrs",params=params, headers={'Content-Type': 'application/json'}, json=update_payload)

#         if response.status_code == 204:
#             print(f"Community {community_Id} updated successfully.")
#             return True
#         else:
#             print(f"Unexpected response code: {response.status_code}")
#             return False
#     except requests.exceptions.RequestException as e:
#         print(f"Error updating community: {e}")
#         return False

def update_community(community_Id, name=None, connection_Details=None, origin=None,
                     role_In_Federation=None, geographical_Location=None,
                     has_Data_Models=None, part_Of_Federation=None, last_Updated=None):
    
    community_urn = f"urn:ngsi-ld:Community:{community_Id}"
    entity_url = f"{context_broker_url}/{community_urn}"
    try:
        response = requests.get(entity_url, headers=headersget)
        response.raise_for_status()
        existing_community = response.json()
        
        print(json.dumps(existing_community, indent=2))
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving community: {e}")
        return False

    update_payload = {"@context": context_url}
    for attr, value in [
        ("name", name),
        ("connectionDetails", connection_Details),
        ("origin", origin),
        ("roleInFederation", role_In_Federation),
        ("geographicalLocation", geographical_Location),
        ("lastUpdated", last_Updated)
    ]:
        if value is not None:
            update_payload[attr] = {"type": "Property", "value": value}
    
    for rel, value in [
        ("hasDataModels", has_Data_Models),
        ("partOfFederation", part_Of_Federation)
    ]:
        if value is not None:
            if isinstance(value, str):
                value = [value]
            if rel == "hasDataModels":
                update_payload[rel] = {"type": "Relationship", "object": [f"urn:ngsi-ld:DataModel:{model}" for model in value]}
                existing_data_models = existing_community.get("hasDataModels", {}).get("object", [])
                models_to_remove = [model for model in existing_data_models if model not in value]
                
                for model_urn in models_to_remove:
                    model_url = f"{context_broker_url}/{model_urn}"
                    try:
                        existing_community["hasDataModels"]["object"].remove(model_urn)
                        params = {'type': 'Community'}
                        response = requests.patch(f"{entity_url}/attrs", params=params, headers=headers, json=existing_community)
                        response.raise_for_status()
                    except requests.exceptions.RequestException as e:
                        print(f"Error removing old model from the community: {e}")
                        return False
                    try:
                        response = requests.get(model_url, headers=headersget)
                        response.raise_for_status()
                        data_model = response.json()
                        data_model["associated_Communities"]["object"].remove(community_urn)
                        params = {'type': 'DataModel'}
                        response = requests.patch(f"{model_url}/attrs", params=params, headers=headers, json=data_model)
                        response.raise_for_status()
                    except requests.exceptions.RequestException as e:
                        print(f"Error updating data model '{model_urn}': {e}")
                        return False

                for model_urn in value: 
                    model_urn = f"urn:ngsi-ld:DataModel:{model_urn}"
                    model_url = f"{context_broker_url}/{model_urn}"
                    try:
                        response = requests.get(model_url, headers=headersget)
                        response.raise_for_status()
                        data_model = response.json()
                        if community_urn not in data_model.get("associated_Communities", {}).get("object", []):
                            if "associated_Communities" not in data_model:
                                data_model["associated_Communities"] = {"type": "Relationship", "object": []}
                            data_model["associated_Communities"]["object"].append(community_urn)
                            params = {'type': 'DataModel'}
                            response = requests.patch(f"{model_url}/attrs", params=params, headers=headers, json=data_model)
                            response.raise_for_status()
                    except requests.exceptions.RequestException as e:
                        print(f"Error updating data model '{model_urn}': {e}")
                        return False
            
            if rel == "partOfFederation":
                update_payload[rel] = {"type": "Relationship", "object": [f"urn:ngsi-ld:Federation:{fed}" for fed in value]}   
                if "partOfFederation" in existing_community and existing_community["partOfFederation"]["object"]:
                    old_federation_urn = existing_community["partOfFederation"]["object"][0]
                    old_federation_url = f"{context_broker_url}/{old_federation_urn}"
                    try:
                        existing_community["partOfFederation"]["object"].remove(old_federation_urn)
                        params = {'type': 'Community'}
                        response = requests.patch(f"{entity_url}/attrs", params=params, headers=headers, json=existing_community)
                        response.raise_for_status()
                    except requests.exceptions.RequestException as e:
                        print(f"Error removing old federation from the community: {e}")
                        return False
                    
                    try:
                        response = requests.get(old_federation_url, headers=headersget)
                        response.raise_for_status()
                        old_federation = response.json()
                        old_federation["includesCommunities"]["object"].remove(community_urn)
                        params = {'type': 'Federation'}
                        response = requests.patch(f"{old_federation_url}/attrs", params=params, headers=headers, json=old_federation)
                        response.raise_for_status()
                    except requests.exceptions.RequestException as e:
                        print(f"Error updating old federation: {e}")
                        return False

                if value:
                    new_federation_urn = value[0]
                    new_federation_url = f"{context_broker_url}/urn:ngsi-ld:Federation:{new_federation_urn}"
                    try:
                        response = requests.get(new_federation_url, headers=headersget)
                        response.raise_for_status()
                        new_federation = response.json()
                        if "includesCommunities" not in new_federation:
                            new_federation["includesCommunities"] = {"type": "Relationship", "object": []}
                        new_federation["includesCommunities"]["object"].append(community_urn)
                        params = {'type': 'Federation'}
                        response = requests.patch(f"{new_federation_url}/attrs", params=params, headers=headers, json=new_federation)
                        response.raise_for_status()
                    except requests.exceptions.RequestException as e:
                        print(f"Error updating new federation: {e}")
                        return False

    try:
        print(json.dumps(update_payload, indent=2))
        params = {'type': 'Community'}
        response = requests.patch(f"{context_broker_url}/{community_urn}/attrs", params=params, headers=headers, json=update_payload)

        if response.status_code == 204:
            print(f"Community {community_Id} updated successfully.")
            return True
        else:
            print(f"Unexpected response code: {response.status_code}")
            print(f"Response content: {response.content.decode('utf-8')}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error updating community: {e}")
        if e.response is not None:
            print(f"Response content: {e.response.content.decode('utf-8')}")
        return False

# success = update_community("Building103", role_In_Federation="updated data provider", part_Of_Federation="e4c_103")
# if success:
#     print("Community updated successfully!")
# else:
#     print("Community update failed.")

# # print(json.dumps(get_list("DataModel"),indent=2))
# success = update_community("communityd",
#                              connection_Details={
#                                 "endpoint":"https://hello.world",
#                                 "protocol":"MQTT"
#                             })
# print(json.dumps(get_federation_by_id("e4c_103"),indent=2))
# success = update_community("Building103",role_In_Federation="updated data provider",
#                             part_Of_Federation="e4c_103")
# if success:
#     print("Community updated successfully!")
# else:
#     print("Community update failed.")
# print(json.dumps(get_community_by_id("Building103"),indent=2))
# print(json.dumps(get_federation_by_id("e4c_103"),indent=2))
# print(json.dumps(get_list("DataModel"),indent=2))

#delete community
def delete_community_by_id(community_Id):
    """
    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    
    entity_url = f"{context_broker_url}/urn:ngsi-ld:Community:{community_Id}"

    try:
        response = requests.delete(entity_url)
        response.raise_for_status()  # Raise an exception for HTTP errors (e.g., 404 Not Found)

        if response.status_code == 204:  # 204 No Content indicates successful deletion
            print(f"Federation with ID '{community_Id}' deleted successfully.")
            return True
        else:
            print(f"Unexpected response code: {response.status_code}")
            return False
    
    except requests.exceptions.RequestException as e:
        print(f"Error deleting Data Model: {e}")
        return False

#test remove community and federation relationship
# print(json.dumps(get_federation_by_id("federationC"),indent=2))
# print(json.dumps(get_community_by_id("communityfinallyy"),indent=2))
# print(remove_federation_community_rel("federationC","communityfinallyy"))
# print(json.dumps(get_federation_by_id("federationC"),indent=2))
# print(json.dumps(get_community_by_id("communityfinallyy"),indent=2))

# ------------------------------------------------------------------------------
# DataModel Management 
# ------------------------------------------------------------------------------
def register_DataModel(dataModel_Id, name, description, model_Format, specific_Ontology,
                    ontology_Version, ontology_URL):
    # Check if the data model already exists
    existing_entity_url = f"{context_broker_url}/urn:ngsi-ld:DataModel:{dataModel_Id}"
    try:
        response = requests.get(existing_entity_url, headers=headers)
        response.raise_for_status()
        if response.status_code == 200:  # Data model exists
            print(f"Data Model {dataModel_Id} already exists!")
            return
    except requests.exceptions.RequestException:
        pass  # If there's an error, assume it doesn't exist
    
    # Construct the NGSI-LD entity
    entity = {
        "id": f"urn:ngsi-ld:DataModel:{dataModel_Id}",
        "type": "DataModel",
        "name": {"type": "Property", "value": name},
        "description": {"type": "Property", "value": description},
        "modelFormat": {"type": "Property", "value": model_Format},
        "ontology": {"type": "Property", "value": specific_Ontology},
        "ontologyVersion": {"type": "Property", "value": ontology_Version},
        "ontologyURL": {"type": "Property", "value": ontology_URL},
        "associated_Communities": {"type": "Relationship", "object": []},
        "@context": [context_url, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]
    }
    # Send POST request to the context broker
    try:
        response = requests.post(context_broker_url, headers=headers, data=json.dumps(entity))
        response.raise_for_status() 
        print(f"Data Model {dataModel_Id} Registered Successfully!")

        # Retrieve and print the registered data model
        
        get_response = requests.get(existing_entity_url, headers = headersget)
        get_response.raise_for_status()
        print("Retrieved Data Model:\n", json.dumps(get_response.json(), indent=2))

    except requests.exceptions.RequestException as e:
        print(f"Error registering Data Model: {e}")
        # Print the detailed error response for debugging
        if e.response is not None:
            print(f"Response content: {e.response.content}")
            
# test register data model
# dataModel_Id = "newdatamodel"
# name = "AirQualitySensorDataModel"
# description = "A data model for air quality sensors"
# model_Format = "JSON"
# specific_Ontology = "SAREF"
# ontology_Version = "4.0.1"
# ontology_URL = "https://www.w3.org/TR/vocab-saref/"

# # Call the function
# register_DataModel(
#     dataModel_Id, name, description, model_Format, specific_Ontology,
#     ontology_Version, ontology_URL
# )

#Get DataModel
def get_data_model_by_id(data_model_id):
    """Retrieves a Data Model entity from the Context Broker by its ID.

    Args:
        data_model_id (str): The ID of the Data Model (e.g., "DM001").

    Returns:
        dict: The JSON representation of the Data Model entity, or None if not found.
    """

    entity_url = f"{context_broker_url}/urn:ngsi-ld:DataModel:{data_model_id}"

    try:
        # Fetch the context JSON from GitHub
        context_response = requests.get(context_url)
        context_response.raise_for_status()  # Raise an error if fetching fails
        context_json = context_response.json()  # Parse the JSON content

        # Prepare headers for the GET request
        # headers = {
        #     "Accept": "application/ld+json",  # Request JSON-LD format
        #     "Link": f'<{context_url}>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
        # }

        response = requests.get(entity_url, headers=headersget)
        response.raise_for_status()  # Raise an exception for HTTP errors

        if response.status_code == 200:
            data_model = response.json()
            return data_model
        else:
            return None

    except requests.exceptions.RequestException as e:
        print(f"Error retrieving Data Model: {e}")
        return None
# print(json.dumps(get_data_model_by_id("newdatamodel"),indent=2))

#Delete DataModel
def delete_data_model_by_id(data_model_id):
    """Deletes a Data Model entity from the Context Broker by its ID.

    Args:
        data_model_id (str): The ID of the Data Model (e.g., "DM001").

    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    
    entity_url = f"{context_broker_url}/urn:ngsi-ld:DataModel:{data_model_id}"

    try:
        response = requests.delete(entity_url)
        response.raise_for_status()  # Raise an exception for HTTP errors (e.g., 404 Not Found)

        if response.status_code == 204:  # 204 No Content indicates successful deletion
            print(f"Data Model with ID '{data_model_id}' deleted successfully.")
            return True
        else:
            print(f"Unexpected response code: {response.status_code}")
            return False
    
    except requests.exceptions.RequestException as e:
        print(f"Error deleting Data Model: {e}")
        return False

#Update DataModel
#========================================================

def update_data_model(data_model_id, name=None, description=None, model_format=None, 
                    ontology=None, ontology_version=None, ontology_url=None):
    entity_url = f"{context_broker_url}/urn:ngsi-ld:DataModel:{data_model_id}"
    try:
        # Fetch existing Data Model using your function
        existing_model = get_data_model_by_id(data_model_id)

        if existing_model is None:
            print(f"Data Model with ID '{data_model_id}' not found.")
            return False

        update_payload = {
            "id": existing_model["id"],
            "type": existing_model["type"],
            "@context": existing_model["@context"]  # Reuse the existing context
        }
        # print(json.dumps(update_payload,indent=2))
        # update_payload={
        #     "@context": existing_model["@context"]
        # }
        # Update properties only if they are provided
        for prop, value in [
            ("name", name),
            ("description", description),
            ("modelFormat", model_format),
            ("ontology", ontology),
            ("ontologyVersion", ontology_version),
            ("ontologyURL", ontology_url),
        ]:
            if value is not None:
                update_payload[prop] = {"type": "Property", "value": value}
                # print(json.dumps(update_payload,indent=2))

        # Send PATCH request (using your global 'headers')
        params = {
            'type': 'DataModel',
        }
        patch_url=f"{entity_url}/attrs"
        print(patch_url)
        response = requests.patch(
            patch_url,params=params, headers={'Content-Type': 'application/ld+json'}, data=json.dumps(update_payload)
        )
        response.raise_for_status()

        if response.status_code == 204:
            print(f"Data Model with ID '{data_model_id}' updated successfully.")
            return True
        else:
            print(f"Unexpected response code: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error updating Data Model: {e}")
        return False
    

# print(update_data_model("model_K",name="name"))

def remove_dataModel_community_rel(community_ID,data_model_ID):
    community_urn=f"urn:ngsi-ld:Community:{community_ID}"
    community_url= f"{context_broker_url}/{community_urn}"
    data_model_urn=f"urn:ngsi-ld:DataModel:{data_model_ID}"
    data_model_url = f"{context_broker_url}/{data_model_urn}"
    try:
        # Fetch existing datamodel
        existing_dataModel = get_data_model_by_id(data_model_ID)
        existing_community=get_community_by_id(community_ID)
        if existing_dataModel and existing_community is None:
            print(f"DataModel with ID '{data_model_ID}'  or community with ID '{community_ID}' not found.")
            return False
        try:
            existing_dataModel["associated_Communities"]["object"].remove(community_urn)
            params = {'type': 'DataModel'}
            response = requests.patch(f"{data_model_url}/attrs",params=params, headers=headers, json=existing_dataModel)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error removing community from datamodel: {e}")
            return False
        try:
            existing_community["hasDataModels"]["object"].remove(data_model_urn)
            params = {'type': 'Community'}
            response = requests.patch(f"{community_url}/attrs",params=params, headers=headers, json=existing_community)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error removing dataModel from the community: {e}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"Error Fetching datamodel or community: {e}")
        return False  
    return True

# print(json.dumps(get_list("DataModel"),indent=2))
#test remove community and datamodel relationship
# print(json.dumps(get_data_model_by_id("AirQuality"),indent=2))
# print(json.dumps(get_community_by_id("communityplz"),indent=2))
# print(remove_dataModel_community_rel("communityplz","AirQuality"))
# print(json.dumps(get_data_model_by_id("AirQuality"),indent=2))
# print(json.dumps(get_community_by_id("communityplz"),indent=2))