# Data Preprocessing and Semantic Integration

## Overview

This directory documents the data acquisition, preprocessing, and semantic transformation workflows implemented across multiple sensor categories within the Di-Hydro project. These processes represent the initial stages of the Di-Hydro data lifecycle, converting raw sensor signals into semantically enriched NGSI-LD entities that are ready for secure exchange, storage, and downstream analytics within the platform.

The workflows described here support a wide range of hydropower plant (HPP) instrumentation, including structural health monitoring, condition monitoring, underwater imaging, water quality sensing, and biosensing. Due to the sensitive or partner-specific nature of some source code and hardware integration details, not all code for the preprocessing pipelines is included here. However, this directory provides:

- A overview of each processing pipeline as documented in Deliverable D2.2
- An illustrative IoT Agent example that transforms structured sensor output (JSON) into NGSI-LD
- Reference to the Di-Hydro semantic data models repository

**For a complete description of the approaches implemented, including the rationale and specific sensor workflows, please refer to Deliverable D2.2 – Section 4.**

## Supported Sensor Categories

The following categories of sensors are integrated into Di-Hydro pilot deployments and have associated preprocessing workflows:

### Structural Health & Condition Monitoring

Sensors:  
- Acoustic Emission (AE)  
- Accelerometers  
- Gyroscopes  
- Strain Gauges  
- Temperature and Humidity  

Workflows include signal acquisition, voltage-to-value transformation, feature extraction (e.g., RMS, amplitude, ASL), and periodic sampling and CSV export. LoRa-based transmission and cloud dashboards are used for visual inspection and data downloads.

### Underwater Inspection Imaging

Involves remote-operated vehicle (ROV) imagery of submerged turbine and intake structures. Preprocessing includes:
- Image enhancement using MSR and enhanced CycleGAN (with CBAM and SiLU)
- Object detection with fine-tuned YOLO (trained on corrosion, cracks, biofouling)
- Outputs enhanced frames and bounding box annotations

### Multiparametric Water Quality Monitoring

Sensor suite includes:
- pH, turbidity, conductivity, dissolved oxygen (DO), ammonia, and algae (Chlorophyll-a)  
- Sensors are integrated into a single multi-parameter platform (MPG-6099)
- CSV outputs contain time-stamped records and sensor-specific metadata
- Initial validation includes range checks, unit normalization, and timestamp parsing

### Escherichia coli Detection 

Uses an electrochemical biosensor with gold leaf electrodes and aptamer biorecognition.  
- Measurements taken via impedance spectroscopy (PicoStat + PSTrace)
- Raw impedance signals processed to extract |Z| at 1 kHz
- Measurement concentration is derived via calibration curves
- Data exported in CSV format and timestamped with sample metadata

### Holographic Imaging and DHM 

- Raw holograms from Digital Holographic Microscopy (DHM) are processed with FFT-based spatial filtering
- Phase reconstruction used for morphological 3D profiling
- Phase/amplitude maps saved and annotated with sample IDs and metadata in structured JSON format

## Semantic Integration via IoT Agents

Once preprocessed, sensor data is transformed into NGSI-LD entities using modular IoT Agents. These agents:

- Ingest structured data from local processing scripts (JSON, CSV)
- Apply semantic mappings using Di-Hydro–specific NGSI-LD data models
- Publish to:
  - **ComDeX** (for real-time publish/subscribe)
  - **Orion-LD + Mintaka** (for persistence and temporal querying)

**All semantic mappings follow the Di-Hydro NGSI-LD Data Models repository:  
🔗 [Di-Hydro Data Models Repository](https://github.com/satrai-lab/di-hydro-data-models)**

The semantic integration enables HPPs to participate in cross-site data exchange securely and interoperably.

## Example: IoT Agent for Multiparametric Water Quality Sensor

We include here an illustrative example of an NGSI-LD-compliant IoT Agent that converts water quality sensor JSON into structured NGSI-LD entities.

### Input Sample (`example_sensor_data.json`)

```json
{
  "timestamp": "2025-05-26T14:00:00Z",
  "device_id": "MPG-6099-001",
  "readings": {
    "pH": 7.2,
    "turbidity": 3.9,
    "conductivity": 750,
    "DO": 5.4
  }
}
```

### Conversion Script (`convert_to_ngsi.py`)

```python
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
```

### Output (`ngsi_entity.json`)

A fully compliant NGSI-LD Observation entity capturing water quality metrics and ready for POST to Orion-LD.

## How to Use

1. Run the conversion:
```bash
python3 convert_to_ngsi.py
```

2. Push the NGSI-LD entity to Orion-LD:
```bash
curl -X POST \
  http://localhost:1026/ngsi-ld/v1/entities \
  -H 'Content-Type: application/ld+json' \
  -d @ngsi_entity.json
```

## Licensing

This repository folder contains public and illustrative materials only.  
Any partner-specific preprocessing code is managed in private repositories or may be shared under bilateral agreements.

The simplified IoT Agent example script provided here follow the MIT license.

## References

- 📄 Deliverable D2.2, Section 4: “Data Acquisition and Processing Algorithms”
- 📚 [FIWARE IoT Agent NGSI-LD Tutorial](https://ngsi-ld-tutorials.readthedocs.io/en/latest/)
- 📚 [Di-Hydro NGSI-LD Data Models](https://github.com/satrai-lab/di-hydro-data-models)

