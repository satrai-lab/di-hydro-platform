# ComDeX Platform

## Overview

This module contains the implementation/code of ComDeX, a lightweight, NGSI-LD-compliant publish/subscribe federation engine, used within the Di-Hydro project to enable secure and semantically interoperable data exchange between Hydro Power Plants (HPPs).

ComDeX provides an MQTT-based communication infrastructure for federated smart environments. It supports real-time dissemination of contextual sensor data using property-graph-based NGSI-LD representations and integrates with other Di-Hydro components such as SHIELD for security, IoT Agents for semantic transformation, and CCDUIT for cross-federation interoperability

  This software component corresponds to the real-time data federation layer described in Deliverable D2.2 – Secure and Transparent Data Exchange Protocols for Optimizing Hydro Power Plant Operations.


**For a deep dive into the ComDeX prototype details, refer to its [wiki](https://satrai-lab.github.io/comdex/).**



## How It Works

The ComDeX platform operates as a federation of ComDeX nodes, each consisting of two primary components: the Action Handler and an MQTT Broker.

### Action Handler

The Action Handler is an API that enables various clients (producers/consumers) to perform diverse "Actions," defined as any operation within the architecture essential for information exchange between clients and brokers. This component facilitates data context discovery (both synchronous and asynchronous) and manages data flows. While ComDeX isn't strictly designed for NGSI-LD, our prototype implementation tries to adhere to NGSI-LD endpoints as closely as possible.

### MQTT Broker

This component serves as the backbone of each ComDeX node. While Mosquitto is used in the examples here our solution isn't confined to this MQTT broker. You can use any MQTT broker, provided it supports message persistence and MQTT bridges creation—two critical features for federation.

**A federation of hydropower plants, each facility runs a Comdex instance so that their devices can exchange data through the Comdex platform.**
![comdex workflow](./images/Comdex%20Workflow.jpeg)

---


##  Installation

### Requirements

Our ComDeX prototype implementation is written in Python, though it's easily adaptable to other programming languages.

- Python environment
- MQTT broker supporting message persistence and MQTT bridges creation

For the Action Handler, you'll need the following libraries:

```
paho-mqtt==1.6.1
Shapely==1.8.1
```
These requirements are included in the requirements.txt file.

### Installation Steps
Install the required libraries with the command: pip install -r requirements.txt.
To view the list of available command-line arguments and their usage, execute 

```python
python3 action_handler.py -h.
```
---

### Sanity Check
To do a quick sanity check that everything has been setup correctly the following can be done:
In the same folder as "actionhandler.py" create 2 files:
  An entity example file, "entity_example.ngsild":
  ```
  {
    "id": "urn:ngsi-ld:GtfsAgency:Malaga_EMT",
    "type": "GtfsAgency",
    "agencyName": {
        "type": "Property",
        "value": "Empresa Malague\u00f1a de Transportes"
    },
    "language": {
        "type": "Property",
        "value": "ES"
    },
    "page": {
        "type": "Property",
        "value": "http://www.emtmalaga.es/"
    },
    "source": {
        "type": "Property",
        "value": "http://datosabiertos.malaga.eu/dataset/lineas-y-horarios-bus-google-transit/resource/24e86888-b91e-45bf-a48c-09855832fd52"
    },
    "timezone": {
        "type": "Property",
        "value": "Europe/Madrid"
    },
    "@context": [
    "https://smartdatamodels.org/context.jsonld",
    "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
    ]
}
  ```
  And a subscription file, subscription_example.ngsild:
  ```
  {
  "id": "urn:subscription:3",
  "type": "Subscription",
  "entities": [{
                "type": "GtfsAgency"
  }],
  "watchedAttributes": ["agencyName","language"],
  "@context": [
    "https://smartdatamodels.org/context.jsonld",
    "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
    ]
}
  ```

We are going to create an entity for which we have subscribed to some of its attributes.
We can do this in any order we like (either publish before or after the subscription).
Replace localhost and 1026 with the appropriate address and port of the deployed MQTT broker.

```
sudo python3 actionhandler.py -c POST/Subscriptions -f subscription_example.ngsild -b localhost -p 1026
sudo python3 actionhandler.py -c POST/entities -f entity_example.ngsild -b localhost -p 1026
```
Subscribed attributes  should now be printed/returned at the terminal





## Enhancing Security for the Comdex Node 

For ComDeX, security extensions have been introduced as part of the Di-Hydro project for its federated data exchange. This includes the implementation of a security layer for the Comdex node by adding authentication and authorization mechanisms.

###  Implementing Authentication and Authorization

To secure the broker, we have introduced **lock** and **unlock** functions, which are administrator-level operations.

#### Lock Function

Locking the broker requires username/password for all connections:

```bash
python3 actionhandler.py --lock=true
```

This prompts for credentials, updates **mosquitto.conf** (`allow_anonymous false`), regenerates **passwd** and **acl**, and reloads the broker.

#### Unlock Function

Re-enable anonymous access:

```bash
python3 actionhandler.py --unlock=true
```

This sets `allow_anonymous true`, comments out auth directives, and reloads the broker.

#### Reloading Mosquitto Broker

The `reload_mosquitto` function checks if Mosquitto is running. If so, it sends a HUP signal to reload; otherwise, it starts the broker with the local config.

###  Example Configuration Files

#### mosquitto.conf (Anonymous Access Enabled)

```conf
# Allow any client to connect without username/password
allow_anonymous true

# Queue size, persistence, etc.
max_queued_messages 4000
persistence true
persistence_location ../data/
log_dest stdout

# This broker listens locally on port 1029
listener 1029 localhost
```

#### mosquitto.conf (Anonymous Access Disabled with Bridge)

```conf
# Require username/password
allow_anonymous false

# Queue size, persistence, etc.
max_queued_messages 4000
persistence true
persistence_location ../data/
log_dest stdout

# This broker listens locally on port 1029
listener 1029 localhost

# Bridge to central broker on port 1026
connection Port_Area_B_Broker_to_Port_Administration_Central_Broker
address localhost:1026
topic provider/# out 2 "" ""

# Bridge auth credentials
remote_username {Remote_broker_username}
remote_password {Remote_broker_password}

# Local auth files
password_file ./passwd
acl_file      ./acl
```

### Collaboration Between Communities

1. **Bridge Configuration**: Node 1 adds `remote_username` and `remote_password` for the central broker.
2. **Password Mapping File**: Both communities maintain **passwd\_mapping.txt** with each broker’s address, port, and credentials.
3. **Publishing Data**: Devices must authenticate or will be rejected and prompted for credentials.
4. **Subscribing to Data**: Users are prompted for credentials if broker requires them.
5. **Retrieving Remote Data**: Comdex node looks up credentials in **passwd\_mapping.txt** to authenticate.

---


## Example: Two Hydropower Plants (HPPa & HPPb)


## Example Project Structure

### Server-side Structure

```bash
comdex-node/
├── actionhandler.py          # Administrative actions (lock/unlock broker)
├── passwd_mapping.txt        # Broker address-port to credentials mapping
├── mosquitto/
│   ├── config/
│   │   ├── mosquitto.conf    # Main broker config
│   │   ├── acl               # Access Control List
│   │   └── passwd            # Password file for client credentials
│   └── data/                 # Persistent info (queues, logs)
```

### Device-side Structure

```bash
device-node/
├── actionhandler.py          # Handle lock/unlock operations
├── passwd_mapping.txt        # Device broker credentials mapping
└── ngsi_ld_files/
    ├── entity_data.json      # Defines device data entities
    └── subscription_data.json# Defines device data subscriptions
```


### Phase 1: Anonymous Access Enabled

1. **Clone Repo**:

   ```bash
   git clone <repo-url> ComdexA
   git clone <repo-url> ComdexB
   ```
2. **Folder Structures**:

   * **ComdexA/** (`mosquitto/config/mosquito.conf`, `passwd_mapping.txt`, `subscription_HydroPowerPlant.ngsild`, `actionhandler.py`, `requirements.txt`)
   * **ComdexB/** (`mosquitto/config/mosquito.conf`, `passwd_mapping.txt`, `HydroPowerPlant.ngsild`, `actionhandler.py`, `requirements.txt`)
3. **Install Dependencies**:
     * Navigate to ComdexA folder (or ComdexB)
   ```bash
   cd ComdexA 
   ```
    * Install the dependencies
   ```bash
   pip install -r requirements.txt
   ```
 
4. **Configure Brokers** (allow\_anonymous true) and start them:   
 * **ComdexA**
    * Open terminal in ComdexA folder and navigate to config folder:
    ```bash
     cd ./mosquitto/config
    ```
    * Run the mosquitto broker server
   ```bash
   sudo mosquitto -c mosquitto.conf
   ```    
 * **ComdexB**
    * Open terminal in ComdexB folder and navigate to config folder:
    ```bash
     cd ./mosquitto/config
    ```
    * Run the mosquitto broker server
   ```bash
   sudo mosquitto -c mosquitto.conf
   ``` 

5. **Publish Data** (HPPb):
    * Open terminal in ComdexB folder and run the following command:
    ```bash
     python3 actionhandler.py --command POST/entities --file HydroPowerPlant.ngsild --broker_address localhost --port 1029
    ```  
6. **Subscribe** (HPPa):
     * Open terminal in ComdexB folder and run the following command:
    ```bash
    python3 actionhandler.py --command POST/Subscriptions --file subscription_HydroPowerPlant.ngsild --broker_address localhost --port 1026
    ```  

### Phase 2: Anonymous Access Disabled 

1. **Lock ComdexA**:

   ```bash
   sudo python3 actionhandler.py --lock=true
   ```

   * Update `passwd_mapping.txt`:
     ```txt
      localhost:1026 {username_comdexA} {password_comdexA}
     ```
2. **Configure Bridge in ComdexB**:

   * Add manually `remote_username` & `remote_password` for the bridge in the `mosquitto.conf` file:
      ```conf
      # Bridge auth credentials 
      remote_username {username_comdexA}
      remote_password {password_comdexA}
      ```
     
   * Update `passwd_mapping.txt`:
     ```txt
      localhost:1029 username_comdexB password_comdexB
      localhost:1026 username_comdexA password_comdexA
     ```
3. **Lock ComdexB**:

   ```bash
   sudo python3 actionhandler.py --lock=true
   ```
4. **Update `passwd_mapping.txt` in ComdexA:**
   * Update `passwd_mapping.txt`:
     ```txt
      localhost:1029 username_comdexB password_comdexB
     ```
5. **Publish Data** (HPPb) using `--username` and `--password` flags:
    * Open terminal in ComdexB folder and run the following command:
    ```bash
     python3 actionhandler.py --command POST/entities --file HydroPowerPlant.ngsild --broker_address localhost --port 1029 --username {username_comdexB} --password {password_comdexB}
    ```  
6. **Subscribe** (HPPa) using `--username` and `--password` flags:
     * Open terminal in ComdexB folder and run the following command:
    ```bash
    python3 actionhandler.py --command POST/Subscriptions --file subscription_HydroPowerPlant.ngsild --broker_address localhost --port 1026 --username {username_comdexA} --password {password_comdexA}
    ``` 
7. **Unlock if Needed using command**:

   ```bash
   sudo python3 actionhandler.py --unlock=true
   ```

---

**Summary**: This setup ensures secure MQTT communication between Comdex nodes with authentication and authorization, enabling safe data exchange between communities.
