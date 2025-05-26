import time
from rdflib import Graph, Literal, RDF, URIRef, Namespace
import json
from rdflib.namespace import RDF, XSD
from datetime import datetime


# Define namespaces
BRICK = Namespace("https://brickschema.org/schema/1.1/Brick#")
EX = Namespace("http://example.com#")

def convert_brick_to_ngsi_ld(ttl_data):
    # start = time.time_ns()
    g = Graph()
    
    # Faster Turtle Parsing
    g.parse(data=ttl_data, format="turtle", publicID="")

    ngsild_data = []

    # Fetch all triples once and store in a dictionary (avoiding multiple lookups)
    observations = {
        obs: {
            "dateObserved": g.value(obs, BRICK["dateObserved"]),
            "occupancyStatus": g.value(obs, BRICK["occupancyStatus"]),
            "occupancyPercentage": g.value(obs, BRICK["occupancyPercentage"]),
            "zonesWithHighOccupancy": g.value(obs, BRICK["zonesWithHighOccupancy"])
        }
        for obs in g.subjects(predicate=None, object=BRICK["OccupancyReading"])
    }

    # Process Observations Efficiently
    for obs, data in observations.items():
        observation_id = f"urn:ngsild:OccupancyReading:{obs.split(':')[-1]}"  # Faster ID extraction
        ngsild_data.append({
            "id": observation_id,
            "type": "OccupancyReading",
            "DateObserved": {"type": "Property", "value": str(data["dateObserved"])},
            "OccupancyStatus": {"type": "Property", "value": str(data["occupancyStatus"])},
            "OccupancyPercentage": {"type": "Property", "value": float(data["occupancyPercentage"]) if data["occupancyPercentage"] else None},
            "ZonesWithHighOccupancy": {"type": "Property", "value": str(data["zonesWithHighOccupancy"]) if data["zonesWithHighOccupancy"] else ""}
        })

    return ngsild_data



def convert_ngsi_ld_to_brick(json_data):
    # Create a graph
    if not json_data:
        raise ValueError("Invalid input: JSON data is None or empty.")
    g = Graph()

    # Define namespaces
    BRICK = Namespace("https://brickschema.org/schema/1.1/Brick#")
    BLDG = Namespace("http://example.org/HydroPowerPlant#")
    g.bind("brick", BRICK)
    g.bind("bldg", BLDG)

    # Extract data from JSON
    observation_id = URIRef(BLDG[json_data["id"].split(":")[-1]])
    community_id = URIRef(json_data["Community"]["object"][0])
    date_observed = Literal(json_data["DateObserved"]["value"])
    Temperature = Literal(json_data["Temperature"]["value"])
    name = Literal(json_data["name"]["value"])

    # Add triples to the graph
    g.add((observation_id, RDF.type, BRICK.OccupancyReading))
    g.add((observation_id, BRICK.isPartOf, community_id))
    g.add((observation_id, BRICK.dateObserved, date_observed))
    g.add((observation_id, BRICK.Temperature, Temperature))
    g.add((observation_id, BRICK.hasName, name))

    # Serialize graph to TTL format
    ttl_data = g.serialize(format="turtle")
    return ttl_data
