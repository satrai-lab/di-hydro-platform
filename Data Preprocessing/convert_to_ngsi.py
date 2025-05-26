import json
from datetime import datetime

with open("example_sensor_data.json") as f:
    data = json.load(f)

entity = {
    "id": f"urn:ngsi-ld:Observation:{data['device_id']}:{data['timestamp']}",
    "type": "Observation",
    "dateObserved": {"type": "Property", "value": data["timestamp"]},
    "deviceId": {"type": "Property", "value": data["device_id"]},
    "measurement": {
        "type": "Property",
        "value": {
            "pH": {"value": data["readings"]["pH"], "unit": "pH"},
            "turbidity": {"value": data["readings"]["turbidity"], "unit": "NTU"},
            "conductivity": {"value": data["readings"]["conductivity"], "unit": "µS/cm"},
            "DO": {"value": data["readings"]["DO"], "unit": "mg/L"}
        }
    },
    "@context": [
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
        "https://raw.githubusercontent.com/satrai-lab/di-hydro-data-models/main/context.jsonld"
    ]
}

with open("ngsi_entity.json", "w") as f_out:
    json.dump(entity, f_out, indent=2)
