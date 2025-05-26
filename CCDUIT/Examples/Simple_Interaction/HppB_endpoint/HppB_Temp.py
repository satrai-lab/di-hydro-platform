import random
import requests
import time
from datetime import datetime
import pytz
import json

# URL for the NGSI-LD API endpoint
context_broker_url = 'http://localhost:1032/ngsi-ld/v1/entities/'  # Replace with actual endpoint if different

# Generate random data
def generate_random_data():
    timestamp = datetime.now(pytz.utc)
    Temperature = random.randint(0, 40)
    
    return {
        "Timestamp": timestamp,
        "Temperature": Temperature
    }

def send_ngsi_ld_observation(observation_data):
    # Convert data to NGSI-LD entity structure with the @context attribute
    # Convert row to NGSI-LD entity structure with the @context attribute
    url_delete=f"{context_broker_url}?type=TemperatureReading"
    response = requests.delete(url_delete)

    # Check if the request was successful
    if response.status_code == 204:
        print("Entities of type 'TemperatureReading' deleted successfully.")
    else:
        print(f"Failed to delete entities. Status code: {response.status_code}, Response: {response.text}")
    observation = {
        "id": f"urn:ngsild:HppB:Observation:{time.time_ns()}",
        "type": "TemperatureReading",
        "name": "Temperature Observation",
        "Community": {
            "type": "Relationship",
            "object": ["urn:ngsi-ld:Community:HppB"]
        },
        "DateObserved": {
            "type": "Property",
            "value": observation_data['Timestamp'].isoformat()
        },
        "Temperature": {
            "type": "Property",
            "value": observation_data['Temperature']
        }
    }

    # Send data to NGSI-LD endpoint
    headers = {
        "Content-Type": "application/json"
    }
    response = requests.post(context_broker_url, headers=headers, data=json.dumps(observation))
    return response.status_code, response.text

# Main simulation loop
while True:
    observation_data = generate_random_data()
    status_code, response_text = send_ngsi_ld_observation(observation_data)
    print(f"Sent data for {observation_data['Timestamp']}: Status {status_code}")
    print(response_text)        
    
    # Wait for 1 second before the next iteration
    time.sleep(1)
