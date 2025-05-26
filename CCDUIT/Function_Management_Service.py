import requests
import json 
import config
import Context_Management_Service
context_broker_url = config.CONTEXT_BROKER_URL  # Replace with your context broker's URL
# context_url="https://raw.githubusercontent.com/NiematKhoder/test/main/Context.json"

# headers = {'Content-Type': 'application/ld+json'}
headers = {'Content-Type': 'application/json'}
# escaped_context = json.dumps(Context).replace('"', '\\"')  # Properly escape quotes
# link_header_value = f'<{json.dumps(context_url).replace(" ", "")}>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
# headersget = {
#             "Accept": "application/ld+json",  # Request JSON-LD format
#             "Link": f'<{context_url}>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
#         }



def register_Function(function_Id, call_Function, description, model_From, model_To,Version,
                    usage_Guide,packages):
    # Check if function already exists
    existing_entity_url = f"{context_broker_url}/urn:ngsi-ld:Function:{function_Id}"
    try:
        response = requests.get(existing_entity_url, headers=headers)
        response.raise_for_status()
        if response.status_code == 200:  # Data model exists
            print(f"Function {function_Id} already exists!")
            return
    except requests.exceptions.RequestException:
        pass  # If there's an error, assume it doesn't exist
    
    # Construct the NGSI-LD entity
    entity = {
        "id": f"urn:ngsi-ld:Function:{function_Id}",
        "type": "Function",
        "callFunction": {"type": "Property", "value": call_Function},
        "description": {"type": "Property", "value": description},
        "modelFrom": {"type": "Property", "value": model_From},
        "modelTo": {"type": "Property", "value": model_To},
        "Version": {"type": "Property", "value": Version},
        "usageGuide": {"type": "Property", "value": usage_Guide},
        "packages": {"type": "Property", "value": packages}
        # "@context": [context_url, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]
    }
    # Send POST request tFunctiono the context broker
    try:
        response = requests.post(context_broker_url, headers=headers, data=json.dumps(entity))
        response.raise_for_status() 
        print(f"Function {function_Id} Registered Successfully!")

        # Retrieve and print the registered data model
        
        # get_response = requests.get(existing_entity_url, headers = headersget)
        # get_response = requests.get(existing_entity_url, headers = headers)
        # get_response.raise_for_status()
        # print("Retrieved Function:\n", json.dumps(get_response.json(), indent=2))

    except requests.exceptions.RequestException as e:
        print(f"Error registering Function: {e}")
        # Print the detailed error response for debugging
        if e.response is not None:
            print(f"Response content: {e.response.content}")

# register_Function("functiontestt","test","from ifc to ngsild","ifc","ngsild","2.1.0","text..",
#                 ["requests","json"])

def get_function_by_id(function_Id):
    entity_url = f"{context_broker_url}/urn:ngsi-ld:Function:{function_Id}"

    try:
        # # Fetch the context JSON from GitHub
        # context_response = requests.get(context_url)
        # context_response.raise_for_status()  # Raise an error if fetching fails
        # context_json = context_response.json()  # Parse the JSON content
        # response = requests.get(entity_url, headers=headersget)
        # response.raise_for_status()  # Raise an exception for HTTP errors

        response = requests.get(entity_url,headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        if response.status_code == 200:
            data_model = response.json()
            return data_model
        else:
            return None

    except requests.exceptions.RequestException as e:
        print(f"Error retrieving Data Model: {e}")
        return None
# print(json.dumps(get_function_by_id("functiontest"),indent=2))

def delete_function_by_id(function_Id):

    entity_url = f"{context_broker_url}/urn:ngsi-ld:Function:{function_Id}"

    try:
        response = requests.delete(entity_url)
        response.raise_for_status()  # Raise an exception for HTTP errors (e.g., 404 Not Found)

        if response.status_code == 204:  # 204 No Content indicates successful deletion
            print(f"Function with ID '{function_Id}' deleted successfully.")
            return True
        else:
            print(f"Unexpected response code: {response.status_code}")
            return False
    
    except requests.exceptions.RequestException as e:
        print(f"Error deleting Data Model: {e}")
        return False
# delete_function_by_id("functiontest")

def update_function(function_Id, call_Function=None, description=None, model_From=None,
                    model_To=None, Version=None, usage_Guide=None, packages=None):
    
    entity_url = f"{context_broker_url}/urn:ngsi-ld:Function:{function_Id}"
    
    try:
        existing_Function = get_function_by_id(function_Id)

        if existing_Function is None:
            print(f"Function with ID '{function_Id}' not found.")
            return False

        update_payload = {}

        for prop, value in [
            ("callFunction", call_Function),
            ("description", description),
            ("modelFrom", model_From),
            ("modelTo", model_To),
            ("Version", Version),
            ("usageGuide", usage_Guide),
            ("packages", packages),
        ]:
            if value is not None:
                if prop == "packages":
                    if "packages" not in existing_Function:
                        update_payload[prop] = {"type": "Property", "value": value}
                    else:
                        update_payload[prop] = {"type": "Property", "value": existing_Function[prop]["value"] + value}
                else:
                    update_payload[prop] = {"type": "Property", "value": value}

        params = {'type': 'Function'}
        patch_url = f"{entity_url}/attrs"
        response = requests.patch(
            patch_url, params=params, headers=headers, data=json.dumps(update_payload)
        )
        response.raise_for_status()

        if response.status_code == 204:
            print(f"Function with ID '{function_Id}' updated successfully.")
            return True
        else:
            print(f"Unexpected response code: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error updating Function: {e}")
        return False

# # Example usage (assuming get_function_by_id is defined):
# print(json.dumps(get_function_by_id("functiontest"), indent=2))
# update_function("functiontest", call_Function="IFC2NGSILD", packages=["argparse"])
# print(json.dumps(get_function_by_id("functiontest"), indent=2))

def check_data_model_mapping(data_model_from, data_model_to):
    """
    Checks if a mapping exists between two data models, based on registered functions.

    Args:
        data_model_from: The source data model (e.g., "ifc").
        data_model_to: The target data model (e.g., "ngsild").
        all_functions: A list of all registered function objects.

    Returns:
        - If a direct mapping exists: The matching function entity.
        - If an indirect mapping exists: A list of function entities forming the chain.
        - If no mapping exists: False.
    """
    all_functions=Context_Management_Service.get_list("Function",headers=headers)
    print(all_functions)
    matching_functions = [func for func in all_functions 
                         if func["modelFrom"]["value"] == data_model_from 
                         and func["modelTo"]["value"] == data_model_to]

    if matching_functions:
        return matching_functions[0]  # Return the first matching function

    # Check for indirect mappings
    for func in all_functions:
        if func["modelFrom"]["value"] == data_model_from:
            intermediate_model = func["modelTo"]["value"]
            mapping_chain = check_data_model_mapping(intermediate_model, data_model_to, all_functions)
            if mapping_chain:
                return [func] + (mapping_chain if isinstance(mapping_chain, list) else [mapping_chain])

    return False  # No mapping exists

# example usage:
# ... your existing code to fetch all_functions ...

# has_mapping = check_data_model_mapping("ifc", "ngsild")

# if has_mapping:
#     if isinstance(has_mapping, list):
#         print("Indirect mapping found:")
#         for func in has_mapping:
#             print(f"  - {func['id']}")
#     else:
#         print(f"Direct mapping found: {has_mapping['id']}")
# else:
#     print("No mapping exists.")
