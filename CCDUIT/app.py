from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from typing import Dict
import importlib.util
import uvicorn
import logging
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional, Dict, Any, Union
import Context_Management_Service as CM
import Function_Management_Service as FM
import Initiate_Collaboration_Service as collab
import config
import Interaction_Handling_Service as  Interaction1
from typing import List
from urllib.parse import quote
from Policy_Management_Service import create_publish_policy as policy_management_create_publish_policy
from Policy_Management_Service import subscribe_retrieve_policy as policy_management_subscribe_retrieve_policy
from Policy_Management_Service import delete_policy as policy_management_delete_policy
from Context_Exchange_Service import store_Federation_Context_based_policy  # Import your function
from contextlib import asynccontextmanager
Policy_Broker_Address = config.FED_BROKER
Policy_Broker_Port = config.FED_PORT
from multiprocessing import Process
import collaboration_monitoring
import policy_monitoring
import re
from datetime import datetime, timezone, timedelta
import json
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
Api_port = 5001  # Use a different port for each federation node

app = FastAPI(
    title="Software Overlay",
    version="0.1.0"
)


docker_compose_path = "./brokers/docker-compose.yml"
config_file_path = "./config.py"
mosquitto_config_path = "./brokers/mosquitto/config/mosquitto.conf"

class UpdateRequest(BaseModel):
    federation_name: str = Field(..., example="Federation1")
    orionLd_port: int = Field(..., example=1052)
    mongo_db_port: int = Field(..., example=27018)
    mosquitto_port: int = Field(..., example=1884)

@app.get("/examples",tags=["Configuration"])
async def get_examples():
    return {
        "example_federation_name": "Federation1",
        "example_ports": {
            "orionLd_port": 1052,
            "mongo_db_port": 27018,
            "mosquitto_port": 1884
        }
    }

@app.put("/update-configuration",tags=["Configuration"])
async def update_configuration(request: UpdateRequest):
    try:
        # Update docker-compose.yml file
        with open(docker_compose_path, "r") as file:
            docker_compose_content = file.read()

        # Replace all instances of "Federation1" with the new federation name
        updated_content = re.sub(r"\bFederation1\b", request.federation_name, docker_compose_content)

        # Update the port number before 1026 for OrionLD
        updated_content = re.sub(
            r"(\d+):1026",
            f"{request.orionLd_port}:1026",
            updated_content
        )

        # Update the port number before 27017 for MongoDB
        updated_content = re.sub(
            r"(\d+):27017",
            f"{request.mongo_db_port}:27017",
            updated_content
        )

        # Update the port number before 1883 for Mosquitto
        updated_content = re.sub(
            r"(\d+):1883",
            f"{request.mosquitto_port}:1883",
            updated_content
        )

        with open(docker_compose_path, "w") as file:
            file.write(updated_content)

        # Update config.py file
        config_content = (
            f"FED_BROKER = \"localhost\"\n"
            f"FED_PORT = {request.mosquitto_port}\n"
            f"CONTEXT_BROKER_URL = \"http://localhost:{request.orionLd_port}/ngsi-ld/v1/entities\"\n"
            f"FEDERATION_ID = \"urn:ngsi-ld:Federation:{request.federation_name}\"\n"
        )

        with open(config_file_path, "w") as file:
            file.write(config_content)

        # Update mosquitto.conf file
        with open(mosquitto_config_path, "r") as file:
            mosquitto_content = file.readlines()

        # Update the listener line
        if mosquitto_content and mosquitto_content[0].startswith("listener"):
            mosquitto_content[0] = f"listener {request.mosquitto_port}\n"

        with open(mosquitto_config_path, "w") as file:
            file.writelines(mosquitto_content)

        return {"message": "Configuration files updated successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/run-monitoring", tags=["Run Monitoring Services"])
def run_monitoring():
    try:
        # Start the collaboration monitoring process
        collaboration_process = Process(target=collaboration_monitoring.main)
        collaboration_process.start()

        # Start the policy monitoring process
        policy_process = Process(target=policy_monitoring.main)
        policy_process.start()

        return {"message": "Monitoring processes started successfully."}
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}

class Community(BaseModel):
    community_Id: str
    name: str
    connection_Details: Dict[str, Any]
    origin: str
    role_In_Federation: str
    geographical_Location: str
    # last_Updated: str
    has_Data_Models: Optional[List[str]] = None
    part_Of_Federation: Optional[str] = None

class Federation(BaseModel):
    federation_Id: str
    name: str
    topology: str
    structure: str
    areaCovered: str
    number_Of_Nodes: int
    part_Of_Federation: Optional[str] = None
    includes_Communities: Optional[List[str]] = None
    uses_Interactions: Optional[List[str]] = None

class DataModel(BaseModel):
    dataModel_Id: str
    name: str
    description: str
    format: str
    specific_Ontology: str
    ontology_Version: str
    ontology_URL: str
    
class Function(BaseModel):
    function_Id: str
    call_Function: str
    description: str
    From_model: str
    To_model: str
    Version: str
    usage_Guide: str
    packages: List[str]

class PartialUpdateCommunity(BaseModel):
    community_Id: Optional[str] = None
    name: Optional[str] = None
    connection_Details: Optional[Dict[str, Any]] = None
    origin: Optional[str] = None
    role_In_Federation: Optional[str] = None
    geographical_Location: Optional[str] = None
    has_Data_Models: Optional[List[str]] = None
    part_Of_Federation: Optional[str] = None
    # last_Updated: Optional[str] = None

class PartialUpdateFederation(BaseModel):
    federation_Id: Optional[str] = None
    name: Optional[str] = None
    topology: Optional[str] = None
    structure: Optional[str] = None
    areaCovered: Optional[str] = None
    number_Of_Nodes: Optional[int] = None
    # part_Of_Federation: Optional[str] = None
    # includes_Communities: Optional[List[str]] = None
    # uses_Interactions: Optional[List[str]] = None

class PartialUpdateDataModel(BaseModel):
    dataModel_Id: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    format: Optional[str] = None
    specific_Ontology: Optional[str] = None
    ontology_Version: Optional[str] = None
    ontology_URL: Optional[str] = None

class PartialUpdateFunction(BaseModel):
    function_Id: Optional[str] = None
    call_Function: Optional[str] = None
    description: Optional[str] = None
    From_model: Optional[str] = None
    To_model: Optional[str] = None
    Version: Optional[str] = None
    usage_Guide: Optional[str] = None
    packages: Optional[List[str]] = None

def get_response_content(request: Request, content: dict):
    headers = request.headers
    if "application/ld+json" in headers.get("accept", ""):
        return JSONResponse(content=content, media_type="application/ld+json")
    return JSONResponse(content=content, media_type="application/json")

@app.post(
    "/federation", 
    tags=["Context Management"],
    response_model=Dict[str, Any],
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "Federation registered successfully"
        }
    },
    response_class=JSONResponse
)
async def register_federation(request: Request, federation: Federation):
    try:
        # Filter out None values
        attributes = {k: v for k, v in federation.dict().items() if v is not None}

        # Call the Context_Management register_Federation function with the filtered attributes
        CM.register_Federation(**attributes)
        return get_response_content(request, {"message": f"Federation {federation.federation_Id} registered successfully!"})
    except Exception as e:
        logger.error(f"Error registering federation: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get(
    "/federations", 
    tags=["Context Management"],
    response_model=List[Federation],
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "List of federations"
        }
    },
    response_class=JSONResponse
)
async def get_federations(request: Request, limit: Optional[int] = None):
    try:
        federations = CM.get_list(type="Federation", limit=limit)
        if federations is not None:
            return get_response_content(request, federations)
        raise HTTPException(status_code=404, detail="Federations not found")
    except Exception as e:
        logger.error(f"Error retrieving federations: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get(
    "/federation/{federation_id}", 
    tags=["Context Management"],
    response_model=Federation,
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "Get federation by ID"
        },
        404: {
            "description": "Federation not found"
        },
        500: {
            "description": "Internal Server Error"
        }
    },
    response_class=JSONResponse
)
async def get_federation_by_id(request: Request, federation_id: str):
    try:
        federation = CM.get_federation_by_id(federation_id)
        if federation is not None:
            return get_response_content(request, federation)
        raise HTTPException(status_code=404, detail=f"Federation {federation_id} not found")
    except Exception as e:
        logger.error(f"Error retrieving federation {federation_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.delete(
    "/federation/{federation_id}", 
    tags=["Context Management"],
    response_model=Dict[str, Any],
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "Federation deleted successfully"
        },
        400: {
            "description": "Failed to delete federation"
        }
    },
    response_class=JSONResponse
)
async def delete_federation_by_id(request: Request, federation_id: str):
    try:
        success = CM.delete_federation_by_id(federation_id)
        if success:
            return get_response_content(request, {"message": f"Federation {federation_id} deleted successfully"})
        raise HTTPException(status_code=400, detail=f"Failed to delete federation {federation_id}")
    except Exception as e:
        logger.error(f"Error deleting federation {federation_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.patch(
    "/federation/{federation_id}",
    tags=["Context Management"], 
    response_model=Dict[str, Any],
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "Federation updated successfully"
        },
        400: {
            "description": "Failed to update federation"
        }
    },
    response_class=JSONResponse
)
async def update_federation(request: Request, federation_id: str, federation: PartialUpdateFederation):
    try:
        # Fetch existing federation
        existing_federation = CM.get_federation_by_id(federation_id)
        if not existing_federation:
            raise HTTPException(status_code=404, detail=f"Federation {federation_id} not found")

        # Prepare the updated fields
        updated_fields = {k: v for k, v in federation.dict().items() if v is not None}
        
        # Call the update function with the updated federation data
        success = CM.update_federation(federation_id, **updated_fields)
        if success:
            return get_response_content(request, {"message": f"Federation {federation_id} updated successfully"})
        raise HTTPException(status_code=400, detail=f"Failed to update federation {federation_id}")
    except Exception as e:
        logger.error(f"Error updating federation {federation_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.post(
    "/community", 
    tags=["Context Management"],
    response_model=Dict[str, Any],
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "Community registered successfully"
        }
    },
    response_class=JSONResponse
)
async def register_community(request: Request, community: Community):
    try:
        success = CM.register_Community(community.community_Id, community.name, community.connection_Details, community.origin,
                                        community.role_In_Federation, community.geographical_Location,  datetime.utcnow().strftime("%Y %H:%M:%S GMT"),
                                        community.has_Data_Models, community.part_Of_Federation)
        if success:
            return get_response_content(request, {"message": f"Community {community.community_Id} registered successfully!"})
        raise HTTPException(status_code=400, detail=f"Failed to register community {community.community_Id}")
    except Exception as e:
        logger.error(f"Error registering community: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
@app.get(
    "/communities", 
    tags=["Context Management"],
    response_model=List[Community],
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "List of communities"
        }
    },
    response_class=JSONResponse
)
async def get_communities(request: Request, limit: Optional[int] = None):
    try:
        communities = CM.get_list(type="Community", limit=limit)
        if communities is not None:
            return get_response_content(request, communities)
        raise HTTPException(status_code=404, detail="Communities not found")
    except Exception as e:
        logger.error(f"Error retrieving Communities: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get(
    "/community/{community_id}", 
    tags=["Context Management"],
    response_model=Community,
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "Get community by ID"
        },
        404: {
            "description": "Community not found"
        },
        500: {
            "description": "Internal Server Error"
        }
    },
    response_class=JSONResponse
)
async def get_community_by_id(request: Request, community_id: str):
    try:
        community = CM.get_community_by_id(community_id)
        if community is not None:
            return get_response_content(request, community)
        raise HTTPException(status_code=404, detail=f"Community {community_id} not found")
    except Exception as e:
        logger.error(f"Error retrieving community {community_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.delete(
    "/community/{community_id}", 
    tags=["Context Management"],
    response_model=Dict[str, Any],
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "Community deleted successfully"
        },
        400: {
            "description": "Failed to delete community"
        }
    },
    response_class=JSONResponse
)
async def delete_community_by_id(request: Request, community_id: str):
    try:
        success = CM.delete_community_by_id(community_id)
        if success:
            return get_response_content(request, {"message": f"Community {community_id} deleted successfully"})
        raise HTTPException(status_code=400, detail=f"Failed to delete community {community_id}")
    except Exception as e:
        logger.error(f"Error deleting community {community_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.patch(
    "/community/{community_id}", 
    tags=["Context Management"],
    response_model=Dict[str, Any],
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "Community updated successfully"
        },
        400: {
            "description": "Failed to update community"
        }
    },
    response_class=JSONResponse
)
async def update_community(request: Request, community_id: str, community: PartialUpdateCommunity):
    try:
        # Fetch existing community
        existing_community = CM.get_community_by_id(community_id)
        if not existing_community:
            raise HTTPException(status_code=404, detail=f"Community {community_id} not found")

        # Prepare the updated fields
        updated_fields = {k: v for k, v in community.dict().items() if v is not None}
        
        # Call the update function with the updated community data
        success = CM.update_community(community_id, **updated_fields)
        if success:
            return get_response_content(request, {"message": f"Community {community_id} updated successfully"})
        raise HTTPException(status_code=400, detail=f"Failed to update community {community_id}")
    except Exception as e:
        logger.error(f"Error updating community {community_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.post(
    "/datamodel", 
    tags=["Context Management"],
    response_model=Dict[str, Any],
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "Data Model registered successfully"
        }
    },
    response_class=JSONResponse
)
async def register_data_model(request: Request, data_model: DataModel):
    try:
        CM.register_DataModel(data_model.dataModel_Id, data_model.name, data_model.description, data_model.format,
                              data_model.specific_Ontology, data_model.ontology_Version, data_model.ontology_URL)
        return get_response_content(request, {"message": f"Data Model {data_model.dataModel_Id} registered successfully!"})
    except Exception as e:
        logger.error(f"Error registering data model: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get(
    "/datamodels", 
    tags=["Context Management"],
    response_model=List[DataModel],
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "List of data models"
        }
    },
    response_class=JSONResponse
)
async def get_datamodels(request: Request, limit: Optional[int] = None):
    try:
        datamodels = CM.get_list(type="DataModel", limit=limit)
        if datamodels is not None:
            return get_response_content(request, datamodels)
        raise HTTPException(status_code=404, detail="data models not found")
    except Exception as e:
        logger.error(f"Error retrieving data models: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get(
    "/datamodel/{data_model_id}", 
    tags=["Context Management"],
    response_model=DataModel,
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "Get Data Model by ID"
        },
        404: {
            "description": "Data Model not found"
        },
        500: {
            "description": "Internal Server Error"
        }
    },
    response_class=JSONResponse
)
async def get_data_model_by_id(request: Request, data_model_id: str):
    try:
        data_model = CM.get_data_model_by_id(data_model_id)
        if data_model is not None:
            return get_response_content(request, data_model)
        raise HTTPException(status_code=404, detail=f"Data Model {data_model_id} not found")
    except Exception as e:
        logger.error(f"Error retrieving data model {data_model_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.delete(
    "/datamodel/{data_model_id}", 
    tags=["Context Management"],
    response_model=Dict[str, Any],
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "Data Model deleted successfully"
        },
        400: {
            "description": "Failed to delete Data Model"
        }
    },
    response_class=JSONResponse
)
async def delete_data_model_by_id(request: Request, data_model_id: str):
    try:
        success = CM.delete_data_model_by_id(data_model_id)
        if success:
            return get_response_content(request, {"message": f"Data Model {data_model_id} deleted successfully"})
        raise HTTPException(status_code=400, detail=f"Failed to delete data model {data_model_id}")
    except Exception as e:
        logger.error(f"Error deleting data model {data_model_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.patch(
    "/datamodel/{data_model_id}", 
    tags=["Context Management"],
    response_model=Dict[str, Any],
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "Data Model updated successfully"
        },
        400: {
            "description": "Failed to update Data Model"
        }
    },
    response_class=JSONResponse
)
async def update_data_model(request: Request, data_model_id: str, data_model: PartialUpdateDataModel):
    try:
        # Fetch existing data model
        existing_data_model = CM.get_data_model_by_id(data_model_id)
        if not existing_data_model:
            raise HTTPException(status_code=404, detail=f"Data Model {data_model_id} not found")

        # Prepare the updated fields
        updated_fields = {k: v for k, v in data_model.dict().items() if v is not None}
        
        # Call the update function with the updated data model data
        success = CM.update_data_model(data_model_id, **updated_fields)
        if success:
            return get_response_content(request, {"message": f"Data Model {data_model_id} updated successfully"})
        raise HTTPException(status_code=400, detail=f"Failed to update data model {data_model_id}")
    except Exception as e:
        logger.error(f"Error updating data model {data_model_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    
@app.delete(
    "/community/{community_id}/datamodel/{data_model_id}", 
    tags=["Context Management"],
    response_model=Dict[str, Any],
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "Data model and community relationship removed successfully"
        },
        400: {
            "description": "Failed to remove data model and community relationship"
        }
    },
    response_class=JSONResponse
)
async def remove_data_model_community_relation(request: Request, community_id: str, data_model_id: str):
    try:
        success = CM.remove_dataModel_community_rel(community_id, data_model_id)
        if success:
            return get_response_content(request, {"message": f"Relationship between Data Model {data_model_id} and Community {community_id} removed successfully"})
        raise HTTPException(status_code=400, detail=f"Failed to remove relationship between Data Model {data_model_id} and Community {community_id}")
    except Exception as e:
        logger.error(f"Error removing data model and community relationship: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.delete(
    "/federation/{federation_id}/community/{community_id}", 
    tags=["Context Management"],
    response_model=Dict[str, Any],
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "Federation and community relationship removed successfully"
        },
        400: {
            "description": "Failed to remove federation and community relationship"
        }
    },
    response_class=JSONResponse
)
async def remove_federation_community_relation(request: Request, federation_id: str, community_id: str):
    try:
        success = CM.remove_federation_community_rel(federation_id, community_id)
        if success:
            return get_response_content(request, {"message": f"Relationship between Federation {federation_id} and Community {community_id} removed successfully"})
        raise HTTPException(status_code=400, detail=f"Failed to remove relationship between Federation {federation_id} and Community {community_id}")
    except Exception as e:
        logger.error(f"Error removing federation and community relationship: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.post(
    "/function", 
    tags=["Context Management"],
    response_model=Dict[str, Any],
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "Function registered successfully"
        }
    },
    response_class=JSONResponse
)
async def register_function(request: Request, function: Function):
    try:
        FM.register_Function(function.function_Id, function.call_Function, function.description, 
                             function.From_model, function.To_model, function.Version,
                             function.usage_Guide, function.packages)
        return get_response_content(request, {"message": f"Function {function.function_Id} registered successfully!"})
    except Exception as e:
        logger.error(f"Error registering function: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get(
    "/functions", 
    tags=["Context Management"],
    response_model=List[Function],
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "List of functions"
        }
    },
    response_class=JSONResponse
)
async def get_datamodels(request: Request, limit: Optional[int] = None):
    try:
        functions = CM.get_list(type="Function", limit=limit,headers = {'Content-Type': 'application/json'})
        if functions is not None:
            return get_response_content(request, functions)
        raise HTTPException(status_code=404, detail="functions not found")
    except Exception as e:
        logger.error(f"Error retrieving functions: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get(
    "/function/{function_id}", 
    tags=["Context Management"],
    response_model=Function,
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "Get function by ID"
        },
        404: {
            "description": "Function not found"
        },
        500: {
            "description": "Internal Server Error"
        }
    },
    response_class=JSONResponse
)
async def get_function_by_id(request: Request, function_id: str):
    try:
        function = FM.get_function_by_id(function_id)
        if function is not None:
            return get_response_content(request, function)
        raise HTTPException(status_code=404, detail=f"Function {function_id} not found")
    except Exception as e:
        logger.error(f"Error retrieving function {function_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.delete(
    "/function/{function_id}", 
    tags=["Context Management"],
    response_model=Dict[str, Any],
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "Function deleted successfully"
        },
        400: {
            "description": "Failed to delete function"
        }
    },
    response_class=JSONResponse
)
async def delete_function_by_id(request: Request, function_id: str):
    try:
        success = FM.delete_function_by_id(function_id)
        if success:
            return get_response_content(request, {"message": f"Function {function_id} deleted successfully"})
        raise HTTPException(status_code=400, detail=f"Failed to delete function {function_id}")
    except Exception as e:
        logger.error(f"Error deleting function {function_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.patch(
    "/function/{function_id}", 
    tags=["Context Management"],
    response_model=Dict[str, Any],
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "Function updated successfully"
        },
        400: {
            "description": "Failed to update function"
        }
    },
    response_class=JSONResponse
)
async def update_function(request: Request, function_id: str, function: PartialUpdateFunction):
    try:
        # Fetch existing function
        existing_function = FM.get_function_by_id(function_id)
        if not existing_function:
            raise HTTPException(status_code=404, detail=f"Function {function_id} not found")

        # Prepare the updated fields
        updated_fields = {k: v for k, v in function.dict().items() if v is not None}
        
        # Call the update function with the updated function data
        success = FM.update_function(function_id, **updated_fields)
        if success:
            return get_response_content(request, {"message": f"Function {function_id} updated successfully"})
        raise HTTPException(status_code=400, detail=f"Failed to update function {function_id}")
    except Exception as e:
        logger.error(f"Error updating function {function_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get(
    "/function/mapping/{data_From_model}/{data_To_model}",
    tags=["Context Management"], 
    response_model=Dict[str, Any],
    responses={
        200: {
            "content": {
                "application/json": {},
                "application/ld+json": {}
            },
            "description": "Mapping checked successfully"
        },
        404: {
            "description": "Mapping not found"
        },
        500: {
            "description": "Internal Server Error"
        }
    },
    response_class=JSONResponse
)
async def check_data_model_mapping(request: Request, data_From_model: str, data_To_model: str):
    try:
        mapping = FM.check_data_model_mapping(data_From_model, data_To_model)
        if mapping:
            return get_response_content(request, {"message": "Mapping exists", "mapping": mapping})
        raise HTTPException(status_code=404, detail=f"No mapping found between {data_From_model} and {data_To_model}")
    except Exception as e:
        logger.error(f"Error checking data model mapping from {data_From_model} to {data_To_model}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


class SharingRule(BaseModel):
    federation: str
    canReceive: bool
    canForward: bool

class Policy(BaseModel):
    policy_ID: str
    name: str
    description: str
    permittedContextTypes: List[str]
    sharingRules: List[SharingRule]
    modifiedBy: str
    Geographic_Restrictions: List[str]

@app.post("/create_publish_policy",tags=["Policy Management"])
def create_publish_policy(policy: Policy):
    sharing_rules = [{rule.federation: {"canReceive": rule.canReceive, "canForward": rule.canForward}} for rule in policy.sharingRules]

    policy_entity = {
        "id": f"urn:ngsi-ld:ContextPolicy:{policy.policy_ID}",
        "type": "ContextPolicy",
        "name": {"type": "Property", "value": policy.name},
        "description": {"type": "Property", "value": policy.description},
        "providerFederation": {"type": "Relationship", "object": config.FEDERATION_ID.split(':')[-1]},
        "permittedContextTypes": {"type": "Property", "value": policy.permittedContextTypes},
        "sharingRules": {"type": "Property", "value": sharing_rules},
        "modificationPolicy": {
            "type": "Property",
            "value": {
                "lastModified": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "modifiedBy": policy.modifiedBy
            }
        },
        "Geographic_Restrictions": {"type": "Property", "value": policy.Geographic_Restrictions}
    }

    file_path = f"{policy.policy_ID}.jsonld"
    with open(file_path, "w") as f:
        json.dump(policy_entity, f, indent=2)

    policy_management_create_publish_policy(policy.policy_ID, policy.name, policy.description, config.FEDERATION_ID.split(':')[-1],
                                            policy.permittedContextTypes, sharing_rules, policy.modifiedBy,
                                            policy.Geographic_Restrictions, Policy_Broker_Address, Policy_Broker_Port)

    return {"status": "Policy created and published successfully"}

@app.get("/retrieve_policy/{federation_id}/{policy_ID}",tags=["Policy Management"])
def retrieve_policy(federation_id: str, policy_ID: str, timeout: int = 1):
    topic = f"Federation/urn:ngsi-ld:Federation:{federation_id}/Policy/urn:ngsi-ld:ContextPolicy:{policy_ID}"
    policy = policy_management_subscribe_retrieve_policy(topic, Policy_Broker_Address, port=Policy_Broker_Port, timeout=timeout)
    if policy:
        return policy
    else:
        raise HTTPException(status_code=404, detail="Policy not found or timeout reached")

@app.delete("/remove_policy/{federation_id}/{policy_ID}",tags=["Policy Management"])
def remove_policy(federation_id: str, policy_ID: str):
    try:
        topic = f"Federation/urn:ngsi-ld:Federation:{federation_id}/Policy/urn:ngsi-ld:ContextPolicy:{policy_ID}"
        policy_management_delete_policy(policy_ID)
        return {"status": f"Policy {policy_ID} under federation {federation_id} removed successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to remove policy {policy_ID}: {str(e)}")


# Pydantic models for request and response
class CollaborationRequest(BaseModel):
    destination_broker: str
    destination_port: int
    receiver_Fed_ID: str
    details: str
    policy_ID: str

class CollaborationResponse(BaseModel):
    id: str
    type: str
    sender: dict
    receiver: dict
    responseTo: dict
    responseStatus: dict
    timestamp: dict
    policyID: Optional[dict] = None

@app.post("/initiate_collaboration/",tags=["Initiate Collaboration"])
def initiate_collaboration(request: CollaborationRequest):
    try:
        # Call the service layer function to send a collaboration request
        collab.send_collaboration_request(
            destination_broker_addr=request.destination_broker,
            destination_port_num=request.destination_port,
            receiver_Fed_ID=request.receiver_Fed_ID,
            details=request.details,
            policy_ID=request.policy_ID
        )
        return {"message": "Collaboration request sent successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/federations/{federation_id}/store_context", status_code=status.HTTP_201_CREATED,
        tags=["Context Exchange after Collaboration"])
async def store_federation_context(federation_id: str):
    """
    Store the context of a federation's policy provider.

    Args:
        federation_id (str): The ID of the federation whose context should be stored.
    """
    try:
        store_Federation_Context_based_policy(federation_id)  # Call your function
    except Exception as e:
        # Handle potential errors and return a meaningful response
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to store context for federation {federation_id}: {e}"
        )

    return {"message": f"Context stored successfully for federation {federation_id}"}


# Pydantic models
class InteractionCreate(BaseModel):
    initiated_By: str
    from_community: str
    towards: str
    Interaction_Type: str
    Interaction_Status: str
    source_data_model: str
    target_data_model: str
    sourcepath: str
    destpath: str

class InteractionStatus(BaseModel):
    interaction_id: str
    connection_Status: str

@app.post("/interactions", response_model=str,
        tags=["Data Interaction Management"])
def create_interaction(interaction: InteractionCreate):
    try:
        interaction_id = Interaction1.create_Interaction(
            interaction.initiated_By,
            interaction.from_community,
            interaction.towards,
            interaction.Interaction_Type,
            interaction.Interaction_Status,
            interaction.source_data_model,
            interaction.target_data_model,
            interaction.sourcepath,
            interaction.destpath
        )
        if interaction_id:
            return interaction_id
        else:
            raise HTTPException(status_code=400, detail="Failed to create interaction")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/interactions/{interaction_id}", response_model=dict,
        tags=["Data Interaction Management"])
def get_interaction(interaction_id: str):
    interaction = Interaction1.get_interaction_by_id(interaction_id)
    if interaction:
        return interaction
    else:
        raise HTTPException(status_code=404, detail="Interaction not found")

@app.get("/interactions/{interaction_id}/status", response_model=str,
        tags=["Data Interaction Management"])
def get_interaction_status(interaction_id: str):
    status = Interaction1.get_interaction_status(interaction_id)
    if status:
        return status
    else:
        raise HTTPException(status_code=404, detail="Interaction status not found")

@app.delete("/interactions/{interaction_id}", response_model=str,
            tags=["Data Interaction Management"])
def remove_interaction(interaction_id: str):
    interaction_id=f"{interaction_id}"
    try:
        Interaction1.remove_Interaction(interaction_id)
        return f"Interaction {interaction_id} removed successfully"
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/interactions", response_model=List[dict],
        tags=["Data Interaction Management"])
def list_interactions():
    try:
        return Interaction1.list_Interactions()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/interactions/{interaction_id}/pause", response_model=str,
        tags=["Data Interaction Management"])
def pause_interaction(interaction_id: str):
    try:
        Interaction1.Update_Interaction(interaction_id, "pause")
        return f"Interaction {interaction_id} paused successfully"
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/interactions/{interaction_id}/resume", response_model=str,
        tags=["Data Interaction Management"])
def resume_interaction(interaction_id: str):
    try:
        Interaction1.Update_Interaction(interaction_id, "resume")
        return f"Interaction {interaction_id} resumed successfully"
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/interactions/{interaction_id}/activate", response_model=str,
        tags=["Data Interaction Management"])
def activate_interaction(interaction_id: str):
    try:
        Interaction1.Update_Interaction(interaction_id, "active")
        return f"Interaction {interaction_id} activated successfully"
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/interactions/{interaction_id}/terminate", response_model=str,
        tags=["Data Interaction Management"])
def terminate_interaction(interaction_id: str):
    try:
        interaction_id=f"{interaction_id}"
        # encoded_interaction_id = quote(interaction_id, safe="")
        Interaction1.terminate_Interaction(interaction_id)
        return f"Interaction {interaction_id} terminated successfully"
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
# Lifespan context manager for startup events
@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"\nUvicorn running on http://127.0.0.1:{Api_port}/docs (Press CTRL+C to quit)\n")
    yield  # This is where FastAPI initializes and runs your app
    print("\nShutting down...")

app.router.lifespan_context = lifespan

# Main section to run the app
if __name__ == "__main__":
    uvicorn.run("app:app", host="127.0.0.1", port=Api_port, log_level="info", reload=True)
    
