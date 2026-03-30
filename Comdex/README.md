# ComDeX Platform

## Overview

This module contains the implementation/code of ComDeX, a lightweight, NGSI-LD-compliant publish/subscribe federation engine, used within the Di-Hydro project to enable secure and semantically interoperable data exchange between Hydro Power Plants (HPPs).

ComDeX provides an MQTT-based communication infrastructure for federated smart environments. It supports real-time dissemination of contextual sensor data using property-graph-based NGSI-LD representations and integrates with other Di-Hydro components such as SHIELD for security, IoT Agents for semantic transformation, and CCDUIT for cross-federation interoperability

  This software component corresponds to the real-time data federation layer described in Deliverable D2.2 – Secure and Transparent Data Exchange Protocols for Optimizing Hydro Power Plant Operations.


**For a deep dive into the ComDeX prototype details, refer to its [wiki](https://satrai-lab.github.io/comdex/).**

## Table of Contents

1. [How It Works](#how-it-works)
   - [Architecture](#architecture)
   - [Data Model — The Topic Namespace](#data-model--the-topic-namespace)
   - [Internal Mechanics](#internal-mechanics)
   - [Federation Flow](#federation-flow)
2. [Installation](#installation)
3. [Project Structure](#project-structure)
4. [Quick Start — Single Node](#quick-start--single-node)
5. [Federating Two Nodes Across Servers](#federating-two-nodes-across-servers)
   - [Phase 1: No Authentication (Anonymous)](#phase-1-no-authentication-anonymous)
   - [Phase 2: Secured with Passwords](#phase-2-secured-with-passwords)
   - [Filtering Which Entity Types Are Advertised](#filtering-which-entity-types-are-advertised)
6. [All Commands Reference](#all-commands-reference)
7. [NGSI-LD File Formats](#ngsi-ld-file-formats)
8. [Security — Lock & Unlock](#security--lock--unlock)
9. [passwd_mapping.txt Format](#passwd_mappingtxt-format)

---

## How It Works

### Architecture

A ComDeX deployment is a **federation of nodes**. Each node has two components:

```
┌─────────────────────────────────┐        ┌─────────────────────────────────┐
│         HPP-A Node              │        │         HPP-B Node              │
│                                 │        │                                 │
│  actionhandler.py  ──────────►  │        │  actionhandler.py  ──────────►  │
│  (CLI / API layer)              │        │  (CLI / API layer)              │
│                                 │        │                                 │
│  Mosquitto Broker               │◄──────►│  Mosquitto Broker               │
│  (data store + message bus)     │  MQTT  │  (data store + message bus)     │
│  port 1026                      │ bridge │  port 1026                      │
└─────────────────────────────────┘        └─────────────────────────────────┘
        192.168.1.10                                 192.168.1.20
```

- **`actionhandler.py`** — the CLI tool. It translates NGSI-LD operations (POST, GET, PATCH, DELETE, Subscriptions) into MQTT publish/subscribe calls against a local or remote broker.
- **Mosquitto broker** — the backbone. It stores all entity data as **retained MQTT messages**, acting as a persistent key-value store. MQTT **bridges** between brokers are how federation works. Any broker supporting persistence and bridges can be used.

---

### Data Model — The Topic Namespace

ComDeX encodes the entire NGSI-LD data model into MQTT topic paths. The broker's retained message store *is* the data store.

**Entity attribute topics** follow this pattern:

```
{area}/entities/{context}/{entityType}/{nodeId}/{entityId}/{attributeName}
```

Each attribute of an entity becomes its own retained MQTT message. Two additional system topics are automatically created per attribute to track timestamps:

```
{area}/entities/{context}/{entityType}/{nodeId}/{entityId}/{attributeName}_timerelsystem_CreatedAt
{area}/entities/{context}/{entityType}/{nodeId}/{entityId}/{attributeName}_timerelsystem_modifiedAt
```

**Example** — publishing a `Turbine` entity with `rpm` and `outputPower` attributes creates these retained messages:

```
unknown_area/entities/https:§§uri.etsi.org§ngsi-ld§v1§ngsi-ld-core-context.jsonld/Turbine/LNA/urn:ngsi-ld:Turbine:001/rpm
unknown_area/entities/https:§§uri.etsi.org§ngsi-ld§v1§ngsi-ld-core-context.jsonld/Turbine/LNA/urn:ngsi-ld:Turbine:001/rpm_timerelsystem_CreatedAt
unknown_area/entities/https:§§uri.etsi.org§ngsi-ld§v1§ngsi-ld-core-context.jsonld/Turbine/LNA/urn:ngsi-ld:Turbine:001/rpm_timerelsystem_modifiedAt
unknown_area/entities/https:§§uri.etsi.org§ngsi-ld§v1§ngsi-ld-core-context.jsonld/Turbine/LNA/urn:ngsi-ld:Turbine:001/outputPower
unknown_area/entities/https:§§uri.etsi.org§ngsi-ld§v1§ngsi-ld-core-context.jsonld/Turbine/LNA/urn:ngsi-ld:Turbine:001/outputPower_timerelsystem_CreatedAt
unknown_area/entities/https:§§uri.etsi.org§ngsi-ld§v1§ngsi-ld-core-context.jsonld/Turbine/LNA/urn:ngsi-ld:Turbine:001/outputPower_timerelsystem_modifiedAt
```

> `§` is used in place of `/` in URLs within the topic path, since `/` is the MQTT topic level separator.

> `LNA` is the node identifier used by default. The `area` defaults to `unknown_area` unless configured via `broker_location_awareness.txt`.

**Provider advertisement topics** form a parallel namespace that acts as a distributed service registry:

```
provider/{brokerAddress}/{port}/{area}/{context}/{entityType}
```

When a node publishes an entity, it also publishes a retained message to this provider topic, advertising that it holds data of that type. Other nodes watch `provider/#` to discover new data sources.

**A federation of hydropower plants, each facility runs a Comdex instance so that their devices can exchange data through the Comdex platform.**
![comdex workflow](./images/Comdex%20Workflow.jpeg)

---
### Internal Mechanics

#### POST /entities — Publishing an Entity

When you run `POST/entities`, the Action Handler:

1. **Checks for duplicates** — subscribes to `+/entities/{context}/{type}/+/{id}/#` for 1 second and listens for any retained messages. If any arrive, the entity already exists and the operation is rejected (use `PATCH` to update instead).
2. **Publishes attribute messages** — each attribute in the JSON is published as a retained MQTT message to its own sub-topic. The payload is the attribute's JSON object. Timestamps (`CreatedAt`, `modifiedAt`) are also published as retained messages.
3. **Publishes a provider advertisement** — a retained message is published to `provider/{broker}/{port}/{area}/{context}/{entityType}` so other nodes can discover this data source. If an advertisement for that entity type already exists on this broker, it is not duplicated.

The result: the broker now holds the entire entity as a set of retained messages. Any client connecting later will immediately receive them without the publisher needing to still be running.

#### POST /Subscriptions — Subscribing to Entity Updates

Subscriptions are **long-running processes**. When you run `POST/Subscriptions`, the Action Handler:

1. **Parses the subscription** — extracts entity `type`, optional `id`, and `watchedAttributes` from the NGSI-LD subscription file.
2. **Publishes the subscription metadata** — records the subscription as a retained message on a subscription topic.
3. **Listens for provider advertisements** — subscribes to `provider/#` on the local broker. This catches:
   - Any providers already advertised (their retained messages arrive immediately).
   - New providers that appear later (as they publish their advertisements).
4. **Spawns a subscriber process per provider** — for each matching advertisement, a new `multiprocessing.Process` is started. That process calls `multiple_subscriptions`, which builds the correct MQTT topic filter based on the subscription's type/id/attributes combination and subscribes to the remote broker that holds the data.

The topic filters built for the actual data subscription depend on what was specified in the subscription:

| Subscription specifies | Topic subscribed to |
|---|---|
| type + watched attributes + id | `{area}/entities/{ctx}/{type}/+/{id}/{attr}` (one per attr) |
| type + id | `{area}/entities/{ctx}/{type}/+/{id}/#` |
| type + watched attributes | `{area}/entities/{ctx}/{type}/+/+/{attr}` (one per attr) |
| type only | `{area}/entities/{ctx}/{type}/#` |
| id only | `{area}/entities/{ctx}/+/+/{id}/#` |
| watched attributes only | `{area}/entities/{ctx}/+/+/+/{attr}` (one per attr) |

5. **Handles provider removal** — if a provider advertisement is deleted (empty payload published to its retained topic), the corresponding subscriber process is killed automatically.

When a subscribed attribute message arrives, the Action Handler reconstructs and prints the NGSI-LD entity JSON to stdout.

#### PATCH /entities — Updating Attributes

`PATCH/entities/{id}/attrs/{attrName}` re-publishes a retained message on the attribute's topic with the new value and updates the `modifiedAt` timestamp. The `CreatedAt` timestamp is not changed.

#### DELETE /entities — Removing an Entity

Deletion works by publishing **empty (null) payloads** to all retained topics for that entity. In MQTT, publishing a zero-length retained message to a topic clears the retained message — the broker discards it. The Action Handler subscribes briefly to find all sub-topics for the entity, then clears each one. It also clears the provider advertisement if no other entities of that type remain on this broker.

---

### Federation Flow

Here is the complete sequence of events when two nodes federate:

```
Server B (192.168.1.20)                     Server A (192.168.1.10)
        │                                           │
        │  [1] HPP-B starts broker with bridge      │
        │      bridge connects to HPP-A             │
        │ ─────────────────────────────────────────►│
        │                                           │
        │  [2] HPP-A runs POST/Subscriptions        │
        │      → subscribes to provider/# locally   │
        │                                           │
        │  [3] HPP-B runs POST/entities             │
        │      → publishes retained attr messages   │
        │      → publishes provider advertisement   │
        │        provider/192.168.1.20/1026/…/Turbine
        │                                           │
        │  [4] bridge forwards provider advert ────►│
        │                                           │  [5] HPP-A's subscription handler
        │                                           │      receives provider advertisement
        │                                           │      → spawns new Process
        │                                           │
        │◄────────────────────────────────────────── [6] Process subscribes directly
        │      to HPP-B's data topics               │      to HPP-B broker
        │                                           │
        │  [7] When HPP-B publishes new values,     │
        │      they arrive at HPP-A's process ─────►│
        │      → printed as NGSI-LD JSON            │
        │                                           │
        │  [8] If provider advert deleted:          │
        │      → bridge forwards empty payload ────►│
        │                                           │  [9] HPP-A kills the subscriber
        │                                           │      process for that provider
```

**Key insight:** the bridge only forwards the `provider/#` namespace — not the actual entity data. The entity data flows directly from HPP-B's broker to HPP-A's subscriber process via a separate MQTT connection. This means entity data never transits through HPP-A's broker; it goes straight to the consuming process.

## Installation

### Requirements

- Python 3.7+
- [Eclipse Mosquitto](https://mosquitto.org/download/) MQTT broker (must support persistence and bridges)

### Steps

```bash
git clone <repo-url> comdex-node
cd comdex-node
pip install -r requirements.txt
```

Verify the installation:

```bash
python3 actionhandler.py -h
```
---

## Quick Start — Single Node

This verifies everything is working before you set up federation.

**1. Start your local Mosquitto broker:**

```bash
cd mosquitto/config
mosquitto -c mosquitto.conf
```

**2. Create a test entity file** `entity_test.json`:

```json
{
  "id": "urn:ngsi-ld:WaterSensor:001",
  "type": "WaterSensor",
  "waterFlow": {
    "type": "Property",
    "value": 42.5
  },
  "temperature": {
    "type": "Property",
    "value": 18.3
  },
  "@context": [
    "https://smartdatamodels.org/context.jsonld",
    "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
  ]
}
```

**3. Create a subscription file** `subscription_test.json`:

```json
{
  "id": "urn:subscription:watersensor-001",
  "type": "Subscription",
  "entities": [
    { "type": "WaterSensor" }
  ],
  "watchedAttributes": ["waterFlow", "temperature"],
  "@context": [
    "https://smartdatamodels.org/context.jsonld",
    "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
  ]
}
```

**4. Subscribe first (in one terminal):**

```bash
python3 actionhandler.py -c POST/Subscriptions -f subscription_test.json -b localhost -p 1026
```

**5. Publish the entity (in another terminal):**

```bash
python3 actionhandler.py -c POST/entities -f entity_test.json -b localhost -p 1026
```

The subscription terminal will print the received attribute values as they arrive.


# Federating Two Nodes Across Servers

This example connects **HPP-A** (Server A, IP `192.168.1.10`) and **HPP-B** (Server B, IP `192.168.1.20`). HPP-B publishes sensor data; HPP-A subscribes to it.

Both servers need ComDeX installed. Mosquitto is running on port `1026` on each.

### Phase 1: No Authentication (Anonymous)

This is the simplest setup — both brokers allow any client to connect without credentials.

#### mosquitto.conf for HPP-A (`192.168.1.10`)

HPP-A does **not** need a bridge — it just receives forwarded advertisements from HPP-B.

```conf
# HPP-A broker — no authentication, no bridge
allow_anonymous true

max_queued_messages 4000
persistence true
persistence_location ./
log_dest stdout

# Listen on all interfaces so HPP-B's bridge can reach us
listener 1026
```

> Note: using `listener 1026` (without an IP) makes Mosquitto accept connections from any interface, not just localhost. This is required for cross-server federation.

#### mosquitto.conf for HPP-B (`192.168.1.20`)

HPP-B has a **bridge** that forwards its provider advertisements to HPP-A.

```conf
# HPP-B broker — no authentication, with bridge to HPP-A
allow_anonymous true

max_queued_messages 4000
persistence true
persistence_location ./
log_dest stdout

# Listen on all interfaces
listener 1026

# Bridge to HPP-A: forward all provider advertisements outbound
connection HPPb_to_HPPa
address 192.168.1.10:1026
topic provider/# out 2 "" ""
```

The `topic provider/# out 2` line means: forward all messages on the `provider/#` namespace **outbound** to HPP-A at QoS 2. This is how HPP-A learns that HPP-B has data available.

#### Starting the Brokers

On each server, navigate to `mosquitto/config/` and run:

```bash
mosquitto -c mosquitto.conf
```

#### Publishing Data from HPP-B

On **Server B** (`192.168.1.20`):

```bash
python3 actionhandler.py -c POST/entities -f entity_test.json -b localhost -p 1026
```

#### Subscribing from HPP-A

On **Server A** (`192.168.1.10`):

```bash
python3 actionhandler.py -c POST/Subscriptions -f subscription_test.json -b localhost -p 1026
```

HPP-A will detect HPP-B's provider advertisement (forwarded via the bridge), then dynamically subscribe to HPP-B's data stream. Matching attribute values will print in the terminal.

---

### Phase 2: Secured with Passwords

In this phase both brokers require authentication. Clients must supply a username and password; bridges must include credentials to connect to the remote broker.

#### Step 1 — Lock HPP-A

On **Server A**, lock the broker. This disables anonymous access and sets up a username/password:

```bash
sudo python3 actionhandler.py --lock=true
```

You will be prompted:
```
Enter new MQTT username: hpp_a_admin
Enter password for hpp_a_admin: ****
```

This automatically:
- Sets `allow_anonymous false` in `mosquitto.conf`
- Creates/updates `mosquitto/config/passwd` with the hashed password
- Creates/updates `mosquitto/config/acl` with full read/write permissions for that user
- Sends `SIGHUP` to reload the running broker (or starts it if not running)

#### Step 2 — Update HPP-B's bridge config

HPP-B's bridge needs credentials to connect to the now-secured HPP-A. Edit `mosquitto/config/mosquitto.conf` on **Server B**:

```conf
# HPP-B broker — no local authentication, secured bridge to HPP-A
allow_anonymous true

max_queued_messages 4000
persistence true
persistence_location ./
log_dest stdout

listener 1026

# Bridge to HPP-A (now secured)
connection HPPb_to_HPPa
address 192.168.1.10:1026
topic provider/# out 2 "" ""

# Credentials for HPP-A's broker
remote_username hpp_a_admin
remote_password <hpp_a_password>
```

Restart HPP-B's broker to apply the new bridge config:

```bash
mosquitto -c mosquitto.conf
```

#### Step 3 — Update passwd_mapping.txt on HPP-B

`passwd_mapping.txt` tells the ComDeX Action Handler which credentials to use when subscribing to data from a remote secured broker. Create or edit this file on **Server B**:

```
192.168.1.10:1026 hpp_a_admin <hpp_a_password>
```

Format: `<broker_address>:<port> <username> <password>` (one entry per line).

#### Step 4 — Lock HPP-B (optional but recommended)

On **Server B**, lock its own broker:

```bash
sudo python3 actionhandler.py --lock=true
```

```
Enter new MQTT username: hpp_b_admin
Enter password for hpp_b_admin: ****
```

Update `passwd_mapping.txt` on **Server B** to also include its own broker (so local clients can authenticate):

```
192.168.1.10:1026 hpp_a_admin <hpp_a_password>
192.168.1.20:1026 hpp_b_admin <hpp_b_password>
```

Update `passwd_mapping.txt` on **Server A** so HPP-A can authenticate when it reaches back to HPP-B:

```
192.168.1.20:1026 hpp_b_admin <hpp_b_password>
```

#### Step 5 — Publish from HPP-B with credentials

On **Server B**:

```bash
python3 actionhandler.py \
  -c POST/entities \
  -f entity_test.json \
  -b localhost \
  -p 1026 \
  --username hpp_b_admin \
  --password <hpp_b_password>
```

#### Step 6 — Subscribe from HPP-A with credentials

On **Server A**:

```bash
python3 actionhandler.py \
  -c POST/Subscriptions \
  -f subscription_test.json \
  -b localhost \
  -p 1026 \
  --username hpp_a_admin \
  --password <hpp_a_password>
```

When HPP-A detects HPP-B's forwarded advertisement and needs to subscribe to HPP-B's data, it automatically looks up HPP-B's credentials in `passwd_mapping.txt`.

#### mosquitto.conf reference — fully secured HPP-A

```conf
# HPP-A broker — authentication enabled
allow_anonymous false

max_queued_messages 4000
persistence true
persistence_location ./
log_dest stdout

listener 1026

# Auth files (auto-managed by actionhandler.py --lock)
password_file ./passwd
acl_file      ./acl
```

#### To re-enable anonymous access

```bash
sudo python3 actionhandler.py --unlock=true
```

This sets `allow_anonymous true` and reloads the broker. The `passwd` and `acl` files are left in place but are no longer enforced.

---

### Filtering Which Entity Types Are Advertised

By default, the bridge line `topic provider/# out 2 "" ""` forwards **all** provider advertisements from HPP-B to HPP-A. This means HPP-A becomes aware of every entity type that HPP-B holds.

You can restrict this to only specific entity types by making the bridge topic filter more specific. The provider advertisement topic structure is:

```
provider/{brokerAddress}/{port}/{area}/{context}/{entityType}
```

The MQTT wildcard `+` matches exactly one topic level, so you can pin specific segments while leaving others as wildcards.

#### Example: advertise only `Turbine` entities

In HPP-B's `mosquitto.conf`, replace the bridge topic line:

```conf
# Before: forward ALL entity type advertisements
topic provider/# out 2 "" ""
```

with:

```conf
# After: forward only Turbine advertisements
topic provider/+/+/+/+/Turbine out 2 "" ""
```

Each `+` wildcard matches one topic level:

```
provider / {brokerAddress} / {port} / {area} / {context} / Turbine
           +                  +         +          +          ← pinned
```

HPP-A will now only learn about `Turbine` data sources on HPP-B. Any `WaterSensor`, `Generator`, or other entity types that HPP-B publishes will not be forwarded and will remain invisible to HPP-A.

#### Example: advertise only a specific area

If your nodes use the `area` segment (configured via `broker_location_awareness.txt`), you can filter by area:

```conf
# Only forward advertisements from the "europe" area
topic provider/+/+/europe/+/+ out 2 "" ""
```

#### Example: advertise multiple specific types

Mosquitto bridges support multiple `topic` lines for the same connection. Add one line per entity type:

```conf
connection HPPb_to_HPPa
address 192.168.1.10:1026

# Only forward Turbine and Generator advertisements
topic provider/+/+/+/+/Turbine    out 2 "" ""
topic provider/+/+/+/+/Generator  out 2 "" ""
```

#### Full HPP-B mosquitto.conf with type filtering

```conf
# HPP-B broker — advertises only Turbine data to HPP-A
allow_anonymous true

max_queued_messages 4000
persistence true
persistence_location ./
log_dest stdout

listener 1026

connection HPPb_to_HPPa
address 192.168.1.10:1026
topic provider/+/+/+/+/Turbine out 2 "" ""
```

> **Why this matters:** in a large federation with many HPPs each holding many entity types, unfiltered bridges can flood every node with advertisements for data they don't need. Filtering at the bridge level keeps the `provider/#` namespace clean and reduces unnecessary cross-node subscription attempts.

---

## All Commands Reference

```
python3 actionhandler.py [options]

Options:
  -h, --help                    Show help and exit
  -c, --command <cmd>           Command to run (see list below)
  -f, --file <path>             Input JSON/NGSI-LD file
  -b, --broker_address <addr>   Broker address (default: localhost)
  -p, --port <port>             Broker port (default: 1026)
  -q, --qos <0|1|2>            MQTT QoS level (default: 0)
  -H, --HLink <context>         Context link for GET requests
  -A, --singleidadvertisement <0|1>  Advertise per entity ID (default: 0)
  -N, --username <user>         MQTT username for authentication
  -S, --password <pass>         MQTT password for authentication
  -K, --lock                    Lock broker (disable anonymous, set credentials)
  -U, --unlock                  Unlock broker (re-enable anonymous access)

Commands:
  POST/entities                 Publish a single NGSI-LD entity
  POST/Subscriptions            Subscribe to an entity type/attribute set
  GET/entities/                 Query entities (supports type, id, attrs, q, geoquery)
  PATCH/entities/               Update specific attributes of an existing entity
  DELETE/entities/              Delete an entity
  entityOperations/create       Batch create — JSON array of entities
  entityOperations/update       Batch update — JSON array of entities
  entityOperations/upsert       Batch upsert (insert or update)
  entityOperations/delete       Batch delete — JSON array of entity IDs
```

### Examples

```bash
# Publish a single entity
python3 actionhandler.py -c POST/entities -f entity.json -b localhost -p 1026

# Publish with authentication
python3 actionhandler.py -c POST/entities -f entity.json -b localhost -p 1026 \
  --username myuser --password mypass

# Subscribe to an entity type
python3 actionhandler.py -c POST/Subscriptions -f subscription.json -b localhost -p 1026

# Query all entities of a type
Do not use GET/ as it supports only local data... 
python3 actionhandler.py -c GET/entities/ -b localhost -p 1026 -H WaterSensor

# Batch create multiple entities
python3 actionhandler.py -c entityOperations/create -f entities_array.json -b localhost -p 1026

# Delete an entity
python3 actionhandler.py -c DELETE/entities/ -f entity.json -b localhost -p 1026

# Lock the broker
sudo python3 actionhandler.py --lock=true

# Unlock the broker
sudo python3 actionhandler.py --unlock=true
```

---

## NGSI-LD File Formats

### Single Entity

```json
{
  "id": "urn:ngsi-ld:Turbine:001",
  "type": "Turbine",
  "rpm": {
    "type": "Property",
    "value": 1500
  },
  "outputPower": {
    "type": "Property",
    "value": 250.7
  },
  "location": {
    "type": "GeoProperty",
    "value": {
      "type": "Point",
      "coordinates": [14.505, 46.056]
    }
  },
  "@context": [
    "https://smartdatamodels.org/context.jsonld",
    "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
  ]
}
```

### Batch Entities (array)

Used with `entityOperations/create`, `entityOperations/update`, `entityOperations/upsert`:

```json
[
  {
    "id": "urn:ngsi-ld:Turbine:001",
    "type": "Turbine",
    "rpm": { "type": "Property", "value": 1500 },
    "@context": ["https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]
  },
  {
    "id": "urn:ngsi-ld:Turbine:002",
    "type": "Turbine",
    "rpm": { "type": "Property", "value": 1480 },
    "@context": ["https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]
  }
]
```

### Subscription

```json
{
  "id": "urn:subscription:turbine-monitor",
  "type": "Subscription",
  "entities": [
    { "type": "Turbine" }
  ],
  "watchedAttributes": ["rpm", "outputPower"],
  "@context": [
    "https://smartdatamodels.org/context.jsonld",
    "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
  ]
}
```

You can also subscribe to a specific entity by ID:

```json
{
  "id": "urn:subscription:turbine-001-monitor",
  "type": "Subscription",
  "entities": [
    { "id": "urn:ngsi-ld:Turbine:001", "type": "Turbine" }
  ],
  "watchedAttributes": ["rpm"],
  "@context": [
    "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
  ]
}
```

---

## Security — Lock & Unlock

### How lock works

`--lock=true` performs the following steps:

1. Prompts for a new username and password
2. Sets `allow_anonymous false` in `mosquitto.conf`
3. Appends `password_file` and `acl_file` directives to `mosquitto.conf`
4. Creates/updates `mosquitto/config/passwd` using `mosquitto_passwd`
5. Creates/updates `mosquitto/config/acl` granting the user full read/write access
6. Sends `SIGHUP` to the running Mosquitto process to reload (or starts it if not running)

Multiple users can be added by running `--lock=true` again with a different username.

### How unlock works

`--unlock=true` sets `allow_anonymous true` and reloads Mosquitto. The `passwd` and `acl` files remain on disk but are no longer active.

### acl file format (auto-generated)

```
user hpp_a_admin
topic readwrite #
```

`topic readwrite #` grants the user full access to all topics. You can restrict this manually if needed.

---

## passwd_mapping.txt Format

This file is read by `actionhandler.py` to automatically authenticate when connecting to remote secured brokers (e.g. during cross-federation GET or subscription operations). It is **not** auto-generated — you maintain it manually.

```
# Format: <broker_address>:<port> <username> <password>
# One entry per line. Lines starting with # are ignored.

192.168.1.10:1026 hpp_a_admin secretpassword
192.168.1.20:1026 hpp_b_admin anotherpassword
```

Place this file in the same directory as `actionhandler.py`.
