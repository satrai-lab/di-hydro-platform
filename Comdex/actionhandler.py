# ComDeX Action Handler Tool

# Version: 0.6.1
# Author: Nikolaos Papadakis 
# Requirements:
# - Python 3.7 or above
# - Shapely library
# - paho-mqtt library

# For more information and updates, visit: [https://github.com/SAMSGBLab/ComDeX]

import sys
import os
import json
import getopt
import subprocess
import time
import threading
import multiprocessing
import re
import ast
import datetime
import shapely.geometry as shape_geo
import urllib.request
from getpass import getpass
import paho.mqtt.client as mqtt
from pickle import TRUE

#default values of mqtt broker to communicate with
default_broker_address='localhost'
default_broker_port=1026
default_ngsild_context="https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"

#global advertisement flag (to avoid for now passing it in every function)
singleidadvertisement=False

#TO DO convert these globals to nonlocals 
#exists=False
exists_topic=''
full_data=''

allow_anonymous   = True
global_username   = None
global_password   = None

# Points to ./mosquitto/config/mosquitto.conf
MOSQ_CONFIG = './mosquitto/config/mosquitto.conf'
# print(MOSQ_CONFIG)

# Points to ./mosquitto/config/passwd
PASSWD_FILE = './mosquitto/config/passwd'
# print(PASSWD_FILE)

# Points to ./mosquitto/config/acl
ACL_FILE = './mosquitto/config/acl'
# print(ACL_FILE)

# Reload the Mosquitto broker if running, otherwise start it with the local configuration.
# Inputs:
#   None
# Returns:
#   None
# Behavior:
#   - If a Mosquitto process is found, sends it a SIGHUP to reload its config.
#   - If no process is running, launches Mosquitto in the background using MOSQ_CONFIG.
def reload_mosquitto():
        try:
            # Check if Mosquitto is already running
            pid = subprocess.check_output(["pidof", "-s", "mosquitto"]).decode().strip()
            subprocess.run(["kill", "-HUP", pid], check=True)
            print(f"🔄 Reloaded existing Mosquitto process (PID: {pid})")
        except subprocess.CalledProcessError:
            # Not running, start it with local config
            subprocess.run(["mosquitto", "-c", str(MOSQ_CONFIG), "-d"])
            print("🚀 Started Mosquitto with local config")    

# ── Lock & secure Mosquitto ───────────────────────────────────────────────
# Description: Lock down the Mosquitto broker (to be run by the administrator): disable anonymous access, prompt for credentials, configure password and ACL files, and reload.
# Inputs:
#   None
# Returns:
#   None
# Behavior:
#   - Prompts the administrator for a new MQTT username and password.
#   - Updates mosquitto.conf to disable anonymous access and reference the local passwd and ACL files.
#   - Creates or updates the passwd file with the new credentials.
#   - Creates or updates the ACL file to grant the new user full topic access.
#   - Reloads (or starts) the Mosquitto broker to apply changes.
def lock_mosquitto():
    global allow_anonymous, global_username, global_password
    allow_anonymous = False

    # 1) Prompt for user/pass
    global_username = input('Enter new MQTT username: ').strip()
    global_password = getpass(f'Enter password for {global_username}: ')

    # 2) Update mosquitto.conf
    conf = []
    with open(MOSQ_CONFIG) as f:
        for line in f:
            if re.match(r'^\s*allow_anonymous\s+', line):
                conf.append('allow_anonymous false\n')
            elif re.match(r'^\s*(password_file|acl_file)\s+', line):
                continue
            else:
                conf.append(line)
    conf += [
        f'password_file ./passwd\n',
        f'acl_file      ./acl\n',
    ]
    with open(MOSQ_CONFIG, 'w') as f:
        f.writelines(conf)

    # 3) Create/update passwd file
    os.makedirs(os.path.dirname(PASSWD_FILE), exist_ok=True)
    # create the passwd file if it doesn't exist
    if not os.path.exists(PASSWD_FILE):
        open(PASSWD_FILE, 'a').close()
    subprocess.run(['mosquitto_passwd', '-b',
                    PASSWD_FILE, global_username, global_password],
                   check=True)

    # 4) Create/update ACL file
    os.makedirs(os.path.dirname(ACL_FILE), exist_ok=True)
    acl = []
    if os.path.exists(ACL_FILE):
        acl = open(ACL_FILE).read().splitlines()
    header = f'user {global_username}'
    if header not in acl:
        acl += [header, 'topic readwrite #']
    with open(ACL_FILE, 'w') as f:
        f.write('\n'.join(acl) + '\n')

    # 5) Reload broker
    reload_mosquitto()
    print('Mosquitto locked down: anonymous disabled, credentials and ACL applied.')

#unlock mosquitto 
# Description: Unlock the Mosquitto broker (to be run by the administrator): re-enable anonymous access, comment out any passwd/ACL or remote auth lines in the config, and reload.
# Inputs:
#   None
# Returns:
#   None
# Behavior:
#   - Sets allow_anonymous to True
#   - In the mosquitto.conf, 
#       • ensures `allow_anonymous true` is set
#       • comments out any `password_file`, `acl_file`, `remote_username`, or `remote_password` directives
#   - Reloads (or starts) the Mosquitto broker to apply changes
def unlock_mosquitto():
    global allow_anonymous
    allow_anonymous = True

    # Read and modify mosquitto.conf
    updated_lines = []
    with open(MOSQ_CONFIG) as f:
        for line in f:
            stripped = line.lstrip()
            # Re-enable anonymous access
            if stripped.startswith('allow_anonymous'):
                updated_lines.append('allow_anonymous true\n')
            # Comment out password_file, acl_file, remote_username, remote_password
            elif stripped.startswith(('password_file', 'acl_file', 'remote_username', 'remote_password')):
                # If already commented, leave as is
                if stripped.startswith('#'):
                    updated_lines.append(line)
                else:
                    updated_lines.append('# ' + line)
            else:
                updated_lines.append(line)

    # Write back the updated configuration
    with open(MOSQ_CONFIG, 'w') as f:
        f.writelines(updated_lines)

    # Reload broker to apply changes
    reload_mosquitto()
    print('Mosquitto unlocked: anonymous access enabled, credential and ACL settings commented out.')



#Function: post_entity
#Description: This function is used to create a new NGSI-LD entity in the ComDeX node.
#Parameters:
#- data: The data of the entity to be created.
#- my_area: The area or domain of the entity.
#- broker: The name or IP address of the broker.
#- port: The port number of the broker.
#- qos: The quality of service level for message delivery.
#- my_loc: The location of the broker (used for advanced advertisements with geoqueries).
#- bypass_existence_check (optional): Flag to bypass the existence check of the entity (default: 0).
#- client (optional): MQTT client object (default: mqtt.Client(clean_session=True)).
#- username (optional): Username for authentication (default: None).
#- password (optional): Password for authentication (default: None).
# Returns: None
def post_entity(data,my_area,broker,port,qos,my_loc,bypass_existence_check=0,client=mqtt.Client(clean_session=True),username=None, password=None):
    

    global singleidadvertisement

    client.loop_start()     
    if 'type' in data:
        typee=str(data['type'])
    else:
        print("Error, ngsi-ld entity without a type \n")
        sys.exit(2)
    if 'id' in data:  
        id=str(data['id'])
    else:
        print("Error, ngsi-ld entity without a id \n")
        sys.exit(2)
    if '@context' in data:
        if( str(type(data["@context"]))=="<class 'str'>"):
            context=data['@context'].replace("/", "§")
        else:
            context=data['@context'][0].replace("/", "§")

        
        
    else:    
        print("Error, ngsi-ld entity without context \n")
        sys.exit(2)
    if 'location' in data:
        location=data['location'] 
    else:
        location=''       
    
    big_topic=my_area+'/entities/'+context+'/'+typee+'/LNA/'+id     

 
    check_topic='+/entities/'+context+'/'+typee+'/+/'+id+'/#' 
    print("Show me the check topic" + check_topic)
    print("Checking existence of entity...")
    
   
    if(bypass_existence_check==0):
        if (check_existence(broker,port,check_topic,username=username,password=password)!=False):
            print("Error entity with this id already exists, did you mean to patch?")
            return

    #check for remote existance maybe in the future
      
    ################### CREATE SMALL TOPICS!!!!!!!!!!!!!!!#######################
    for key in data.items():
        if key[0]!="type" and key[0]!="id" and key[0]!='@context':
            
            small_topic=my_area+'/entities/'+context+'/'+typee+'/LNA/'+id+'/'+key[0]
            #print(small_topic)
            print("Publishing message to subtopic")    
            
            client.publish(small_topic,str(key[1]),retain=True,qos=qos)
            
            curr_time=str(datetime.datetime.now())
            time_rels = { "createdAt": [curr_time],"modifiedAt": [curr_time] }

            small_topic=my_area+'/entities/'+context+'/'+typee+'/LNA/'+id+'/'+ key[0]+"_timerelsystem_CreatedAt"
                
            client.publish(small_topic,str(time_rels["createdAt"]),retain=True,qos=qos)
            
            small_topic=my_area+'/entities/'+context+'/'+typee+'/LNA/'+id+'/'+ key[0]+"_timerelsystem_modifiedAt"
            client.publish(small_topic,str(time_rels["modifiedAt"]),retain=True,qos=qos)     

    ############################################################################
    check_topic2="provider/+/+/"+my_area+'/'+context+'/'+typee+'/'
    
    if(singleidadvertisement==False):
        special_context_provider_broadcast= 'provider/' + broker + '/' +str(port) + '/'+my_area+'/' + context + '/' +typee
    else:
        special_context_provider_broadcast= 'provider/' + broker + '/' +str(port) + '/'+my_area+'/' + context + '/' +typee +'/'+id
        bypass_existence_check=1
    
    if(bypass_existence_check==1):
        print("Bypassing existence check for advertisement")
        client.publish(special_context_provider_broadcast,"Provider Message: { CreatedAt:" + str(time_rels["createdAt"]) +",location:" + str(my_loc)+"}" ,retain=True,qos=2)
        print(special_context_provider_broadcast) 
    elif(check_existence(broker,port,special_context_provider_broadcast,username=username,password=password)==False):
        print("checking existence of advertisement...")
        info= client.publish(special_context_provider_broadcast,"Provider Message: { CreatedAt:" + str(time_rels["createdAt"]) +",location:" + str(my_loc)+"}" ,retain=True,qos=2)
        # Check the result code
        if info.rc == 0:
            print("Publishing message to provider table")
            print(special_context_provider_broadcast)     
        else:
            print(f"Failed to send publish request to {special_context_provider_broadcast}. Return code: {info.rc}")

          
        #old logging of published messages
        #logger = logging.getLogger()
        #handler = logging.FileHandler('logfile_advertisement_published.log')
        #logger.addHandler(handler)
        #logger.error(time.time_ns()/(10**6))

           
    client.loop_stop()


#Description: This function checks if an entity or advertisement already exists inside the broker.
#Parameters:
#- broker: The name or IP address of the broker.
#- port: The port number of the broker.
#- topic: The topic name or identifier of the message to check.
#- username (optional): Username for authentication (default: None).
#- password (optional): Password for authentication (default: None).
#Returns:
#- True if the entity/advertisement exists in the broker, False otherwise.

def check_existence(broker,port,topic, username=None, password=None):
    print("checking existence of topic: " + topic + " to the broker: " + broker + " on port: " + str(port) + "using username: " + str(username) + " and password: " + str(password))
    run_flag=TRUE
    exists=False
    expires=1
    def on_connect(client3, userdata, flags, rc):
        print("Connected for existence check with result code "+str(rc))

    # The callback for when a PUBLISH message is received from the server.
    def on_message(client3, userdata, msg):
        global exists_topic
        nonlocal exists
        nonlocal expires
        exists=True
        exists_topic=msg.topic
        expires-=1
    
    client3 = mqtt.Client()   
    client3.username_pw_set(username, password) if username and password else None
    client3.on_connect = on_connect
    client3.on_message = on_message

    
    client3.connect(broker, port)
    client3.loop_start()
    client3.subscribe(topic,qos=1)

    start=time.perf_counter()
    try:
        while run_flag:
            tic_toc=time.perf_counter()
            if (tic_toc-start) > expires:
                run_flag=False
    except:
        pass
    #time.sleep(1)
    client3.loop_stop()  
    #print(exists)
    return exists    


# Function: GET
# Description: This function is used to retrieve entities from the ComDeX node, similar to the NGSI-LD GET entities operation.
# Parameters:
#   - broker: The name or IP address of the broker.
#   - port: The port number of the broker.
#   - topics: A list of topics to subscribe to.
#   - expires: The expiration time in seconds.
#   - qos: The quality of service level for message delivery.
#   - limit (optional): The maximum number of entities to retrieve (default: 2000).
#   - username (optional): Username for authentication (default: None).
#   - password (optional): Password for authentication (default: None).
# Returns:
#   - A list of received messages (entities) ordered via their id.

def GET(broker, port, topics, expires, qos, limit=2000,username=None, password=None):
    run_flag = True
    messagez = []
    messages_by_id = {}

    # The callback for when a PUBLISH message is received from the server.
    def on_message(client, userdata, msg):
        nonlocal messagez
        nonlocal expires
        nonlocal messages_by_id
        nonlocal limit
        if msg.retain == 1:
            initial_topic = msg.topic.split('/')
            id = initial_topic[-2]
            messages_by_id.setdefault(id, []).append(msg)
            if len(messages_by_id) == limit + 1:
                expires -= 10000000
            else:
                messagez.append(msg)
                expires += 0.5

    # Create an MQTT client
    client = mqtt.Client()
    client.username_pw_set(username, password) if username and password else None
    client.on_message = on_message
    client.connect(broker, port)

    client.loop_start()

    # Subscribe to the specified topics
    for topic in topics:
        client.subscribe(topic, qos)

    start = time.perf_counter()

    try:
        while run_flag:
            tic_toc = time.perf_counter()
            if tic_toc - start > expires:
                run_flag = False
    except:
        pass

    client.loop_stop()

    # Return the received messages (entities)
    return messagez


# Function: recreate_single_entity
# Description: This function recreates a single entity from the received messages based on the specified query conditions.
# This is possible because each entity has a unique message id, which is used as the catalyst for the entity reconstruction from
# its various attribute messages
# Parameters:
#   - messagez: List of received messages (entities).
#   - query: Query condition to filter the entities (optional, default: '').
#   - topics: Topic filters to apply (optional, default: '').
#   - timee: Time condition to filter the entities (optional, default: '').
#   - georel: Geo-relation condition to filter the entities (optional, default: '').
#   - geometry: Geometry type for the geospatial condition (optional, default: '').
#   - coordinates: Coordinates for the geospatial condition (optional, default: '').
#   - geoproperty: Geospatial property for the geospatial condition (optional, default: '').
#   - context_given: Context value for entity comparison (optional, default: '').
# Returns: None

def recreate_single_entity(messagez, query='', topics='', timee='', georel='', geometry='', coordinates='', geoproperty='', context_given=''):
    query_flag_passed = False
    subqueries_flags = {}
    default_context = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"

    # Extract initial topic information
    initial_topic = (messagez[0].topic).split('/')
    id = initial_topic[-2]
    typee = initial_topic[-4]
    context = initial_topic[-5]
    context = context.replace("§", "/")
    context_text = context
    contextt = []
    contextt.append(context_text)

    # Add default context if it differs from the specified context
    if context_text != default_context:
        contextt.append(default_context)

    # Initialize data dictionary with ID and type
    data = {}
    data['id'] = id
    data['type'] = typee

    # Check if a specific context is given for comparison
    if context_given == '+':
        with urllib.request.urlopen(context_text) as url:
            data_from_web = json.loads(url.read().decode())
        try:
            data['type'] = data_from_web["@context"][typee]
        except:
            dummy_command = "This is a dummy command for except"

    if query == '':
        for msg in messagez:
            attr_str = msg.payload
            attr_str = attr_str.decode(encoding='UTF-8', errors='strict')
            attr_str = attr_str.replace("\'", "\"")
            data2 = json.loads(attr_str)
            topic = (msg.topic).split('/')

            # Check geospatial condition if specified
            if georel != '':
                if topic[-1] == geoproperty:
                    geo_type = str(data2["value"]["type"])
                    geo_coord = str(data2["value"]["coordinates"])
                    geo_ok = 0

                    geo_type = geo_type.replace(" ", "")
                    geo_coord = geo_coord.replace(" ", "")
                    coordinates = coordinates.replace(" ", "")

                    geo_entity = shape_geo.shape((data2["value"]))

                    if geometry == "Point":
                        query_gjson = shape_geo.Point(json.loads(coordinates))
                    elif geometry == "LineString":
                        query_gjson = shape_geo.LineString(json.loads(coordinates))
                    elif geometry == "Polygon":
                        query_gjson = shape_geo.Polygon(json.loads(coordinates))
                    elif geometry == "MultiPoint":
                        query_gjson = shape_geo.MultiPoint(json.loads(coordinates))
                    elif geometry == "MultiLineString":
                        query_gjson = shape_geo.MultiLineString(json.loads(coordinates))
                    elif geometry == "MultiPolygon":
                        query_gjson = shape_geo.MultiPolygon(json.loads(coordinates))

                    # Check specific georelation condition
                    if georel == "equals":
                        if geo_entity.equals(query_gjson):
                            geo_ok = 1
                        else:
                            return
                    elif georel == "within":
                        if geo_entity.within(query_gjson):
                            geo_ok = 1
                        else:
                            return
                    elif georel == "intersects":
                        if geo_entity.intersects(query_gjson):
                            geo_ok = 1
                        else:
                            return
                    elif re.search("near;", georel):
                        near_query = georel.split(';')
                        near_operator = re.findall('[><]|==|>=|<=', near_query[1])
                        near_geo_queries = (re.split('[><]|==|>=|<=', near_query[1]))

                        if str(near_geo_queries[0]) == "maxDistance":
                            if str(near_operator[0]) == "==":
                                if geo_entity.distance(query_gjson) > float(near_geo_queries[1]):
                                    return
                        elif str(near_geo_queries[0]) == "minDistance":
                            if str(near_operator[0]) == "==":
                                if geo_entity.distance(query_gjson) < float(near_geo_queries[1]):
                                    return
                    elif georel == "contains":
                        if geo_entity.contains(query_gjson):
                            geo_ok = 1
                        else:
                            return
                    elif georel == "disjoint":
                        if geo_entity.disjoint(query_gjson):
                            geo_ok = 1
                        else:
                            return
                    elif georel == "overlaps":
                        if geo_entity.overlaps(query_gjson):
                            geo_ok = 1
                        else:
                            return

            # Check topic filters if specified
            if topics != '' and topics != "#":
                if topic[-1] in topics:
                    data[topic[-1]] = data2
                if topic[-1].endswith("_CreatedAt") or topic[-1].endswith("_modifiedAt"):
                    if timee != '':
                        time_topic = (topic[-1].split('_timerelsystem_'))
                        if context_given == '+':
                            try:
                                time_topic[-2] = data_from_web["@context"][time_topic[-2]]
                            except:
                                dummy_command = "This is a dummy command for except"

                        data[time_topic[-2]][time_topic[-1]] = data2

            else:
                if topic[-1].endswith("_CreatedAt") or topic[-1].endswith("_modifiedAt"):
                    if timee != '':
                        time_topic = (topic[-1].split('_timerelsystem_'))
                        if context_given == '+':
                            try:
                                time_topic[-2] = data_from_web["@context"][time_topic[-2]]
                            except:
                                dummy_command = "This is a dummy command for except"

                        data[time_topic[-2]][time_topic[-1]] = data2
                else:
                    data[topic[-1]] = data2

        data['@context'] = contextt

        json_data = json.dumps(data, indent=4, ensure_ascii=False)
        print(json_data)

    elif query != '':
        logical_operators = re.findall('[;|()]', query)
        queries_big = re.split(('[;|()]'), query)

        for msg in messagez:
            attr_str = msg.payload
            attr_str = attr_str.decode(encoding='UTF-8', errors='strict')
            attr_str = attr_str.replace("\'", "\"")
            data2 = json.loads(attr_str)
            topic = (msg.topic).split('/')

            # Check geospatial condition if specified
            if georel != '':
                if topic[-1] == geoproperty:
                    geo_type = str(data2["value"]["type"])
                    geo_coord = str(data2["value"]["coordinates"])
                    geo_ok = 0

                    geo_type = geo_type.replace(" ", "")
                    geo_coord = geo_coord.replace(" ", "")
                    coordinates = coordinates.replace(" ", "")

                    geo_entity = shape_geo.shape((data2["value"]))

                    if geometry == "Point":
                        query_gjson = shape_geo.Point(json.loads(coordinates))
                    elif geometry == "LineString":
                        query_gjson = shape_geo.LineString(json.loads(coordinates))
                    elif geometry == "Polygon":
                        query_gjson = shape_geo.Polygon(json.loads(coordinates))
                    elif geometry == "MultiPoint":
                        query_gjson = shape_geo.MultiPoint(json.loads(coordinates))
                    elif geometry == "MultiLineString":
                        query_gjson = shape_geo.MultiLineString(json.loads(coordinates))
                    elif geometry == "MultiPolygon":
                        query_gjson = shape_geo.MultiPolygon(json.loads(coordinates))

                    # Check specific georelation condition
                    if georel == "equals":
                        if geo_entity.equals(query_gjson):
                            geo_ok = 1
                        else:
                            return
                    elif georel == "within":
                        if geo_entity.within(query_gjson):
                            geo_ok = 1
                        else:
                            return
                    elif georel == "intersects":
                        if geo_entity.intersects(query_gjson):
                            geo_ok = 1
                        else:
                            return
                    elif re.search("near;", georel):
                        near_query = georel.split(';')
                        near_operator = re.findall('[><]|==|>=|<=', near_query[1])
                        near_geo_queries = (re.split('[><]|==|>=|<=', near_query[1]))

                        if str(near_geo_queries[0]) == "maxDistance":
                            if str(near_operator[0]) == "==":
                                if geo_entity.distance(query_gjson) > float(near_geo_queries[1]):
                                    return
                        elif str(near_geo_queries[0]) == "minDistance":
                            if str(near_operator[0]) == "==":
                                if geo_entity.distance(query_gjson) < float(near_geo_queries[1]):
                                    return
                    elif georel == "contains":
                        if geo_entity.contains(query_gjson):
                            geo_ok = 1
                        else:
                            return
                    elif georel == "disjoint":
                        if geo_entity.disjoint(query_gjson):
                            geo_ok = 1
                        else:
                            return
                    elif georel == "overlaps":
                        if geo_entity.overlaps(query_gjson):
                            geo_ok = 1
                        else:
                            return

            # Allowing combination of logical queries
            for query2 in queries_big:
                operator = re.findall('[><]|==|>=|<=', query2)
                queries = (re.split('[><]|==|>=|<=', query2))
                subqueries_flags.setdefault(queries[0], False)

                if queries[0] == topic[-1]:

                    if str(operator[0]) == "==":

                        if isinstance(data2["value"], list):
                            for data3 in data2["value"]:
                                if data3 == queries[1]:
                                    subqueries_flags[queries[0]] = True

                        elif data2["value"] == queries[1]:
                            subqueries_flags[queries[0]] = True
                    elif queries[1].isnumeric():
                        if str(operator[0]) == ">":
                            if float(data2["value"]) > float(queries[1]):
                                subqueries_flags[queries[0]] = True
                        elif str(operator[0]) == "<":
                            if float(data2["value"]) < float(queries[1]):
                                subqueries_flags[queries[0]] = True
                        elif str(operator[0]) == "<=":
                            if float(data2["value"]) <= float(queries[1]):
                                subqueries_flags[queries[0]] = True
                        elif str(operator[0]) == ">=":
                            if float(data2["value"]) >= float(queries[1]):
                                subqueries_flags[queries[0]] = True

            # Check topic filters if specified
            if topics != '' and topics != "#":
                if topic[-1] in topics:
                    data[topic[-1]] = data2
                if topic[-1].endswith("_CreatedAt") or topic[-1].endswith("_modifiedAt"):
                    if timee != '':
                        time_topic = (topic[-1].split('_timerelsystem_'))
                        if context_given == '+':
                            try:
                                time_topic[-2] = data_from_web["@context"][time_topic[-2]]
                            except:
                                dummy_command = "This is a dummy command for except"

                        data[time_topic[-2]][time_topic[-1]] = data2

            else:
                if topic[-1].endswith("_CreatedAt") or topic[-1].endswith("_modifiedAt"):
                    if timee != '':
                        time_topic = (topic[-1].split('_timerelsystem_'))
                        if context_given == '+':
                            try:
                                time_topic[-2] = data_from_web["@context"][time_topic[-2]]
                            except:
                                dummy_command = "This is a dummy command for except"

                        data[time_topic[-2]][time_topic[-1]] = data2
                else:
                    data[topic[-1]] = data2

        data['@context'] = contextt

        l = 0
        full_logical_equation = []
        subqueries_flags.pop('', None)
        for results in subqueries_flags.values():
            full_logical_equation.append(str(results))
            if l < (len(logical_operators)):
                if logical_operators[l] != '':
                    if logical_operators[l] == ";":
                        full_logical_equation.append('and')
                    elif logical_operators[l] == "|":
                        full_logical_equation.append('or')
                    else:
                        full_logical_equation.append(logical_operators[l])
                    if l + 1 < (len(logical_operators) - 1):
                        while logical_operators[l + 1] != ';' and logical_operators[l + 1] != '|':
                            l = l + 1
                            full_logical_equation.append(logical_operators[l])

            l = l + 1

        query_flag_passed = eval(' '.join(full_logical_equation))
        if query_flag_passed == True:
            json_data = json.dumps(data, indent=4, ensure_ascii=False)
            print(json_data)
        
                 



        
# Function: recreate_multiple_entities
# Description: This function recreates multiple entities from the received messages based on the specified query conditions. It basically calls the 
# recreate single entity command, over a list of MQTT messages, based on their id.
# Parameters:
#   - messagez: List of received messages (entities).
#   - query: Query condition to filter the entities (optional, default: '').
#   - topics: Topic filters to apply (optional, default: '').
#   - timee: Time condition to filter the entities (optional, default: '').
#   - limit: Maximum number of entities to recreate (optional, default: 2000).
#   - georel: Geo-relation condition to filter the entities (optional, default: '').
#   - geometry: Geometry type for the geospatial condition (optional, default: '').
#   - coordinates: Coordinates for the geospatial condition (optional, default: '').
#   - geoproperty: Geospatial property for the geospatial condition (optional, default: '').
#   - context_given: Context value for entity comparison (optional, default: '').
# Returns: None

def recreate_multiple_entities(messagez, query='', topics='', timee='', limit=2000, georel='', geometry='', coordinates='', geoproperty='', context_given=''):
    messages_by_id = {}

    # Separate each message by ID to recreate using the recreate_single_entity function
    for message in messagez:
        initial_topic = (message.topic).split('/')
        id = initial_topic[-2]
        messages_by_id.setdefault(id, []).append(message)

    # Iterate over single entities and recreate them using recreate_single_entity function
    for single_entities in messages_by_id.values():
        recreate_single_entity(single_entities, query, topics, timee, georel, geometry, coordinates, geoproperty, context_given)

        # Countdown pagination limit
        limit = limit - 1
        if limit == 0:
            break




        

# Function: multiple_subscriptions
# Description: This function sets up multiple subscriptions based on the specified flags and parameters.
# Parameters:
#   - entity_type_flag: Flag indicating whether entity type is specified.
#   - watched_attributes_flag: Flag indicating whether watched attributes are specified.
#   - entity_id_flag: Flag indicating whether entity ID is specified.
#   - area: Area value for the subscriptions.
#   - context: Context value for the subscriptions.
#   - truetype: Entity type value for the subscriptions.
#   - true_id: Entity ID value for the subscriptions.
#   - expires: Expiration time for the subscriptions.
#   - broker: MQTT broker address.
#   - port: MQTT broker port.
#   - qos: Quality of Service level for the subscriptions.
#   - watched_attributes: List of watched attributes for the subscriptions.
# Returns: None

def multiple_subscriptions(entity_type_flag, watched_attributes_flag, entity_id_flag, area, context, truetype, true_id, expires, broker, port, qos, watched_attributes):
    topic = []
    
    if entity_type_flag and watched_attributes_flag and entity_id_flag:
        # Subscribe to topics based on entity type, watched attributes, and entity ID
        for attr in watched_attributes:
            topic.append(area + '/entities/' + context + '/' + truetype + '/+/' + true_id + '/' + attr)
    
    elif entity_type_flag and entity_id_flag:
        # Subscribe to topics based on entity type and entity ID
        topic.append(area + '/entities/' + context + '/' + truetype + '/+/' + true_id + '/#')
    
    elif watched_attributes_flag and entity_id_flag:
        # Subscribe to topics based on watched attributes and entity ID
        for attr in watched_attributes:
            topic.append(area + '/entities/' + context + '/+/+/' + true_id + '/' + attr)
    
    elif entity_type_flag and watched_attributes_flag:
        # Subscribe to topics based on entity type and watched attributes
        for attr in watched_attributes:
            topic.append(area + '/entities/' + context + '/' + truetype + '/+/+/' + attr)
    
    elif entity_type_flag:
        # Subscribe to topics based on entity type
        topic.append(area + '/entities/' + context + '/' + truetype + '/#')
    
    elif entity_id_flag:
        # Subscribe to topics based on entity ID
        topic.append(area + '/entities/' + context + '/+/+/' + true_id + '/#')
    
    elif watched_attributes_flag:
        # Subscribe to topics based on watched attributes
        for attr in watched_attributes:
            topic.append(area + '/entities/' + context + '/+/+/+/' + attr)
    
    else:
        print("Something has gone wrong, program did not find the topics to subscribe to!")
        sys.exit(2)
    
    # Call the subscribe function with the generated topics
    subscribe(broker, port, topic, expires, qos, context_given=context)
    



# Function: subscribe
# Description: This function subscribes to MQTT topics and handles the received messages.
# Parameters:
#   - broker: MQTT broker address.
#   - port: MQTT broker port.
#   - topics: List of topics to subscribe to.
#   - expires: Expiration time for the subscriptions.
#   - qos: Quality of Service level for the subscriptions.
#   - context_given: Context value for entity comparison.
# Returns: None

def subscribe(broker, port, topics, expires, qos, context_given):
    run_flag = True
   
   # Description: Look up MQTT credentials for a given broker address and port from passwd_mapping.txt.
   # Inputs:
   #   broker_address (str): hostname or IP of the MQTT broker
   #   port (int): TCP port number of the broker
   # Returns:
   #   tuple (username (str), password (str)) if found, otherwise (None, None)
    def read_credentials(broker_address, port):
        try:
            with open('passwd_mapping.txt', 'r') as f:
                for line in f:
                    parts = line.strip().split()
                    if len(parts) != 3:
                        continue
                    addr_port, username, password = parts
                    addr, prt = addr_port.split(':')
                    if addr == broker_address and int(prt) == port:
                        return username, password
        except FileNotFoundError:
            print("Error: passwd_mapping.txt file not found.")
        return None, None

    # The callback for when the client receives a CONNACK response from the server.
    # This is called when the client connects to the broker.
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected successfully.")
        else:
            print(f"Connection failed with return code {rc}")
    

    # Description: Create and configure an MQTT client, trying credentialed connection first and falling back to anonymous.
    # Inputs:
    #   broker_address (str): hostname or IP address of the MQTT broker to connect to
    #   port (int): TCP port number on which the broker is listening
    # Returns:
    #   mqtt.Client instance on success, otherwise None
    # Behavior:
    #   - Retrieve credentials via read_credentials()
    #   - If present, use them; otherwise attempt anonymous connect
    #   - Start network loop on successful connect
    def connect_with_logic(broker_address, port):
        client = mqtt.Client() 

        username, password = read_credentials(broker_address, port)

        if username and password:
            client.username_pw_set(username, password)
            print(f"Trying to connect to {broker_address}:{port} with credentials from file...")
            try:
                client.connect(broker_address, port)
                client.loop_start()
                return client
            except Exception as e:
                print(f"Error: Connection with credentials failed. Details: {e}")
                return None
        else:
            print(f"No credentials found in file for {broker_address}:{port}. Trying anonymous connection...")
            try:
                client.connect(broker_address, port)
                client.loop_start()
                return client
            except Exception as e:
                print(f"Anonymous connection also failed. Please provide credentials in passwd_mapping.txt.")
                return None




    # The callback for when a PUBLISH message is received from the server.
    def on_message(client, userdata, msg):
        # Perform operations on received message
        if msg.payload.decode() != '':
            stamp = time.time_ns() / (10 ** 6)
            initial_topic = (msg.topic).split('/')
            ids = initial_topic[-2]
            tmp = msg.payload
            attr_str = msg.payload.decode(encoding='UTF-8', errors='strict')
            attr_str = attr_str.replace("\'", "\"")
            try:
                data2 = json.loads(attr_str)
            except:
                print("Error decoding JSON data")
                return

            #Older logging messages for experiments
            # Logging the message arrival time difference
            #handler = logging.FileHandler('logfile_mqtt_notification_arrived.log')
            # logger.addHandler(handler)
            # logger.error(str(stamp - float(data2["value"])))

        if msg.payload.decode() != '':
            messagez = []
            messagez.append(msg)
            if msg.topic.endswith("_CreatedAt") or msg.topic.endswith("_modifiedAt"):
                # Do nothing for special system time topics (ignore them, no temporal subscriptions)
                do_nothing = 1
            else:
                # Recreate single entity based on the received message
                recreate_single_entity(messagez, timee=0, context_given=context_given)
        else:
            print("\n Message on topic:" + msg.topic + ", was deleted")

    client = connect_with_logic(broker, port)
    if client is None:
        print("Failed to connect to the broker. Exiting...")
        sys.exit(2)
        # return
    
    client.on_message = on_message

    # Connect to MQTT broker
    client.connect(broker, port, keepalive=expires)

    client.loop_start()
    for topic in topics:
        client.subscribe(topic, qos)
        #print("Subscribing to topic: " +topic)

    start = time.perf_counter()

    try:
        while run_flag:
            tic_toc = time.perf_counter()
            if (tic_toc - start) > expires:
                run_flag = False
    except:
        pass

    print("Subscriptions expired, exiting.....")
    # Blocking call that processes network traffic, dispatches callbacks and handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a manual interface.
    client.loop_stop()

# Function: subscribe_for_advertisement_notification
# Description: This function sets up subscriptions for advertisement notifications based on the specified flags and parameters, basically to
# find if a new advertisement of interest arrives while an intersted subscriber is active (so that the subscriber while also connect to the
# new advertised source "on the fly").
# Parameters:
#   - broker: MQTT broker address.
#   - port: MQTT broker port.
#   - topics: List of topics to subscribe to for advertisement notifications.
#   - expires: Expiration time for the subscriptions.
#   - qos: Quality of Service level for the subscriptions.
#   - entity_type_flag: Flag indicating whether entity type is specified.
#   - watched_attributes_flag: Flag indicating whether watched attributes are specified.
#   - entity_id_flag: Flag indicating whether entity ID is specified.
#   - watched_attributes: List of watched attributes for the subscriptions.
#   - true_id: Entity ID value for the subscriptions.
#   - username (optional): Username for authentication (default: None).
#   - password (optional): Password for authentication (default: None).
# Returns: None

def subscribe_for_advertisement_notification(broker, port, topics, expires, qos, entity_type_flag, watched_attributes_flag, entity_id_flag, watched_attributes, true_id,username=None, password=None):
    run_flag = True
    advertisement_exists = {}
    jobs_to_terminate = {}
    print(topics)
    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(client, userdata, flags, rc):
        print(f"Connecting to MQTT broker at {broker}:{port} {'with credentials' if username else 'anonymously'}")
        print("Connected for advertisement notification with result code " + str(rc))
        if rc == 5:
            print("Authentication failed: invalid username or password.")
            client.loop_stop()
            print("MQTT authentication failed with result code 5 (Not authorized)")
            sys.exit(2)
            
            
        elif rc != 0:
            print(f"Connection failed with result code {rc}")
            client.loop_stop()
            print(f"MQTT connection failed with code {rc}")
            sys.exit(2)

    # The callback for when a PUBLISH message is received from the server.
    def on_message(client, userdata, msg):
        print("MESSAGE advert HERE")
        print(msg.payload.decode())
        if msg.payload.decode() != '':
            # Calculate at which time an interested subscriber received an advertisement message
            # that did not exist prior to its subscription
            nonlocal advertisement_exists

            stamp = time.time_ns() / (10 ** 6)

            initial_topic = (msg.topic).split('/')
            broker_remote = initial_topic[1]
            port_remote = int(initial_topic[2])
            area_remote = initial_topic[3]
            context = initial_topic[4]
            truetype = initial_topic[5]

            topic2 = "provider/" + broker_remote + '/' + str(port_remote) + '/' + area_remote + '/' + context + '/' + truetype
            attr_str = topic2
            print(attr_str)

            if topic2 in advertisement_exists.keys():
                print("advertisement_already_exists")
                return ()
            else:
                print("found_brand_new_advertisement")
                advertisement_exists.setdefault(topic2, [])

            topic = []

            context_providers_addresses = []
            context_providers_ports = []
            context_providers_areas = []
            number_of_threads = 1

            context_providers_addresses.append(initial_topic[1])
            context_providers_ports.append(initial_topic[2])
            context_providers_areas.append(initial_topic[3])

            nonlocal jobs_to_terminate
            jobs = []
            for i in range(0, number_of_threads):
                #print("How many threads???")
                process = multiprocessing.Process(
                    target=multiple_subscriptions,
                    args=(
                        entity_type_flag, watched_attributes_flag, entity_id_flag,
                        context_providers_areas[i], context, truetype, true_id, expires,
                        context_providers_addresses[i], int(context_providers_ports[i]), qos, watched_attributes
                    )
                )
                jobs.append(process)

            print(jobs)
            jobs_to_terminate.setdefault(topic2, jobs)
            jobs_to_terminate[topic2] = jobs
            for j in jobs:
                print(j)
                j.start()

        else:
            initial_topic = (msg.topic).split('/')
            broker_remote = initial_topic[1]
            port_remote = int(initial_topic[2])
            area_remote = initial_topic[3]
            context = initial_topic[4]
            truetype = initial_topic[5]

            topic2 = "provider/" + broker_remote + '/' + str(port_remote) + '/' + area_remote + '/' + context + '/' + truetype
            print("Advertisement Deleted")
            advertisement_exists.pop(topic2, 1)
            print(jobs_to_terminate)
            for j in jobs_to_terminate[topic2]:
                print(j)
                j.kill()

    client = mqtt.Client()
    print("username and password")
    print(username, password)
    client.username_pw_set(username, password) if username and password else None
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker, port, keepalive=expires)

    client.loop_start()
    for topic in topics:
        client.subscribe(topic, qos)
        print("Subscribing to topic: " + topic)

    start = time.perf_counter()

    try:
        while run_flag:
            tic_toc = time.perf_counter()
            if (tic_toc - start) > expires:
                run_flag = False
    except:
        pass

    print("Subscriptions expired, exiting.....")
    client.loop_stop()


# Function: clear_retained
# Description: This function clears the retained messages on the specified topic(s).
# Parameters:
#   - broker: MQTT broker address.
#   - port: MQTT broker port.
#   - retained: Single topic or list of topics to clear retained messages from.
#   - username (optional): Username for authentication (default: None).
#   - password (optional): Password for authentication (default: None).
# Returns: None

def clear_retained(broker, port, retained,username=None, password=None):
    run_flag = True
    expires = 0.5

    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(client, userdata, flags, rc):
        print("Connected with result code " + str(rc))

    # The callback for when a PUBLISH message is received from the server.
    def on_message(client, userdata, msg):
        nonlocal expires

        if (msg.retain == 1):
            # Publish a null message to clear the retained message
            client2.publish(msg.topic, None, 0, True)
            print("Clearing retained on topic -", msg.topic)
            expires += 0.1

    client = mqtt.Client()
    client2 = mqtt.Client()
    client.username_pw_set(username, password) if username and password else None
    client2.username_pw_set(username, password) if username and password else None
    client.on_connect = on_connect
    client2.on_connect = on_connect
    client.on_message = on_message

    client.connect(broker, port)
    client2.connect(broker, port)
    client.loop_start()
    client2.loop_start()
    client.subscribe(retained, qos=1)

    start = time.perf_counter()
    try:
        while run_flag:
            tic_toc = time.perf_counter()
            if (tic_toc - start) > expires:
                run_flag = False
    except:
        pass

    client.loop_stop()
    client2.loop_stop()


#debug functions to see mqtt broker communication
############
# Function: on_message
# Description: Callback function called when a message is received from the MQTT broker.
# Parameters:
#   - client: MQTT client instance.
#   - userdata: Custom userdata provided by the client.
#   - message: MQTT message object containing the received message.
# Returns: None

def on_message(client, userdata, message):
    # Print the received message payload, topic, QoS, and retain flag
    print("message received ", str(message.payload.decode("utf-8")))
    print("message topic=", message.topic)
    print("message qos=", message.qos)
    print("message retain flag=", message.retain)

# Function: on_log
# Description: Callback function called when a log message is generated by the MQTT client.
# Parameters:
#   - client: MQTT client instance.
#   - userdata: Custom userdata provided by the client.
#   - level: Log level of the message.
#   - buf: Log message string.
# Returns: None

def on_log(client, userdata, level, buf):
    # Print the log message
    print("log: ", buf)
#########################################   




def usage():
    print("\nUsage:")
    print("python actionhandler.py [options]\n")
    
    print("Options:")
    print("-h, --help                Show this help message and exit")
    print("  -L, --lock                Lock and secure Mosquitto (disable anonymous, set credentials)")
    print("  -U, --unlock   Re-enable anonymous and reload Mosquitto")
    print("-c, --command             Provide the command, possible commands include [POST/entities,POST/Subscriptions,DELETE/entities/,PATCH/entities/,GET/entities/,entityOperations/delete,entityOperations/create,entityOperations/update,entityOperations/upsert]")
    print("-f, --file                Specify the file to be used as input to the command")
    print("-b, --broker_address      Specify the address of the MQTT broker of the ComDeX node")
    print("-p, --port                Specify the port number of the MQTT broker of the ComDeX node")
    print("-q, --qos                 Specify the Quality of Service level (0, 1, or 2) to be used for the specified command")
    print("-H, --HLink               Specify the HLink, 'context link' to be used for the GET request")
    print("-A, --singleidadvertisement Specify if single ID advertisement is to be used (use 1 for True), default is false")
    print("-N, --username               Username for MQTT broker (if credentials are required)")
    print("-S, --password               Password for MQTT broker (if credentials are required)")

    print("\nExample without authentication:")
    print("python3 actionhandler.py -c POST/entities -f entity.ngsild -b localhost -p 1026 -q 1 -H HLink -A 0\n")

    print("Example with authentication:")
    print("python3 actionhandler.py -c POST/entities -f entity.ngsild -b localhost -p 1026 -q 1 -H HLink -A 0 -N username -S password\n")



#ComDeX is an ngsild compliant "broker" that utilises a running MQTT broker
#Here is the main function where different functions are called mainly based on the selected by the user command.

def main(argv):

    try:
        opts, args = getopt.getopt(argv,"hc:f:b:p:l:q:H:A:K:U:N:S:",["command=","file=","broker_address=","port=","qos=","HLink=","singleidadvertisement=","lock=","unlock=","username=","password="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    # Initialize defaults
    command = ''
    file = ''
    HLink = ''
    qos = 0
    Forwarding = 1
    broker = default_broker_address
    port = default_broker_port
    expires = 3600
    global singleidadvertisement
    singleidadvertisement = False
    my_area = "unknown_area"
    my_loc = "unknown_location"
    lock_flag = False
    unlock_flag = False
    username = None
    password = None
    # Parse ComDeX flags
    for opt, arg in opts:
        if opt == '-h':
            usage(); sys.exit()
        elif opt in ("-c", "--command"):
            command = arg
        elif opt in ("-f", "--file"):
            file = arg
        elif opt in ("-b", "--broker_address"):
            broker = arg
        elif opt in ("-p", "--port") or opt == '-l':
            port = int(arg)
        elif opt in ("-q", "--qos"):
            qos = int(arg)
            if qos < 0 or qos > 2:
                print("Invalid Mqtt qos"); sys.exit(2)
        elif opt in ("-H", "--HLink"):
            HLink = arg
        elif opt in ("-A", "--singleidadvertisement"):
            singleidadvertisement = (arg == "1")
        elif opt in ("-K","--lock"):
            lock_flag = True
        elif opt in ("-U","--unlock"):
            unlock_flag = True
        elif opt in ("-N","--username"):
            username = arg
        elif opt in ("-S","--password"):
            password = arg
    # print(lock_flag,unlock_flag)        
    if lock_flag:
            lock_mosquitto()
            print("Mosquitto is now locked")
    if unlock_flag:
            unlock_mosquitto()
            print("Mosquitto is now unlocked")
            
    # Broker location awareness
    open("broker_location_awareness.txt", "a+").close()
    with open("broker_location_awareness.txt", "r") as f:
        contents = f.read()
    try:
        location_awareness = ast.literal_eval(contents)
        area_key = f"{broker}:{port}:area"
        loc_key  = f"{broker}:{port}:loc"
        if area_key in location_awareness:
            my_area = location_awareness[area_key]
        if loc_key in location_awareness:
            my_loc = location_awareness[loc_key]
    except:
        pass

    if not command:
        print("No command found, exiting..."); sys.exit(2)
    
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected successfully.")
        else:
            print(f"Connection failed with return code {rc}")
   
    def connect_mqtt(broker_address, port, username=None, password=None):
        client = mqtt.Client(clean_session=True)
        client.on_connect = on_connect

        if username and password:
            client.username_pw_set(username, password)
            print(f"Trying to connect to {broker_address}:{port} with credentials...")
            try:
                client.connect(broker_address, port)
                client.loop_start()
                return client
            except Exception as e:
                print("Provide correct username and password.")
                return None
        else:
            print(f"Trying anonymous connection to {broker_address}:{port}...")
            try:
                client.connect(broker_address, port)
                client.loop_start()
                return client
            except Exception as e:
                print("Anonymous connection failed. Provide username and password.")
                return None

    # POST ENTITIES
    if command == "POST/entities":
        print("creating new instance")
        client = connect_mqtt(broker, port, username=username, password=password)
        client.on_message = on_message
        if not file:
            usage(); sys.exit(2)

        print("NGSI-LD POST entity command detected")
        with open(file) as jf:
            try:
                data = json.load(jf)
            except json.JSONDecodeError:
                print("Can't parse the input file, are you sure it is valid JSON?")
                sys.exit(2)
        if client is None:
            print("Failed to connect to the broker. Exiting...")
            return
        post_entity(data, my_area, broker, port, qos, my_loc, 0, client,username=username,password=password)

    # CREATION OF SUBSCRIPTIONS
    elif command == 'POST/Subscriptions':
        
        truetype=''; true_id=''; entity_type_flag=False; watched_attributes_flag=False; entity_id_flag=False; watched_attributes=''
        if not file: usage(); sys.exit(2)
        print("ngsild Post Subscription command detected")
        with open(file) as jf:
            try: data=json.load(jf)
            except: print("Can't parse the input file, are you sure it is valid json?"); sys.exit(2)
        if data.get('type')!='Subscription': print(f"Subscription has invalid type: {data.get('type')}"); sys.exit(2)
        sid = str(data['id']) if 'id' in data else (print("Error, ngsi-ld Subscription without a id "), sys.exit(2))[0]
        ctx = data.get('@context');
        if isinstance(ctx,str): context=ctx.replace('/','§')
        else: context=ctx[0].replace('/','§') if ctx else '+'
        if 'entities' in data:
            ie=data['entities'][0]
            if 'type' in ie: truetype=str(ie['type']); entity_type_flag=True
            if 'id' in ie:   true_id=str(ie['id']);   entity_id_flag=True
        if 'watchedAttributes' in data:
            watched_attributes=data['watchedAttributes']; watched_attributes_flag=True
            if watched_attributes is None: print("Watched attributes without content, exiting...."); sys.exit(2)
        expires=int(data.get('expires',expires))
        if not (entity_type_flag or watched_attributes_flag or entity_id_flag): print("Error, ngsi-ld subscription without information about topics, exiting.... "); sys.exit(2)
        big_topic=f"{my_area}/Subscriptions/{context}/Subscription/LNA/{sid}"

        client1=connect_mqtt(broker,port,username=username,password=password)
        if client1 is None:
            print("Failed to connect to the broker. Exiting...")
            return
        client1.on_message=on_message; 
        client1.publish(big_topic,str(data),qos=qos); client1.loop_stop()
        area=data.get('area',['+']); truetype2=truetype or '#'; trueid2=true_id or '#'; check_top=[]
        for z in area:
            if not singleidadvertisement: check_top.append(f"provider/+/+/{z}/{context}/{truetype2}")
            else:                      check_top.append(f"provider/+/+/{z}/{context}/{truetype}/{trueid2}")
        subscribe_for_advertisement_notification(broker,port,check_top,expires,qos,entity_type_flag,watched_attributes_flag,entity_id_flag,watched_attributes,true_id,username=username,password=password)

    # DELETE/entities/
    elif re.search(r"DELETE/entities/",command):
        username =username
        password = password
        client=connect_mqtt(broker,port,username=username,password=password)
        if client is None:
            print("Failed to connect to the broker. Exiting...")
            return
        parts=command.split('/')
        if len(parts)<5:
            H=HLink.replace('/','§') if HLink else '+'; eid=parts[2]
            top=f"{my_area}/entities/{H}/+/+/{eid}/#"
            if not check_existence(broker,port,top,username=username,password=password ): print("Entity with this id doesn't exist, no need for deletion"); sys.exit(2)
            clear_retained(broker,port,top,username=username,password=password)
            tp=exists_topic.split('/')[-4]; tc=f"{my_area}/entities/{H}/{tp}/+/+/#"
            if not singleidadvertisement:
                spec=f"provider/{broker}/{port}/{my_area}/{H}/{tp}"
                if not check_existence(broker,port,tc,username=username,password=password): clear_retained(broker,port,spec,username=username,password=password)
            else: spec=f"provider/{broker}/{port}/{my_area}/{H}/{tp}/{eid}"; clear_retained(broker,port,spec,username=username,password=password)
        else:
            H=HLink.replace('/','§') if HLink else '+'
            if parts[3]!='attrs': print("Please check delete attr cmd"); sys.exit(2)
            eid=parts[2]; top=f"{my_area}/entities/{H}/+/+/{eid}/{parts[4]}"; clear_retained(broker,port,top,username=username,password=password)

    # PATCH/entities/
    elif re.search(r"PATCH/entities/",command):
        client=connect_mqtt(broker,port,username=username,password=password)
        if client is None:
            print("Failed to connect to the broker. Exiting...")
            return
        
        H=HLink.replace('/','§') if HLink else '+'; parts=command.split('/')
        if len(parts)<5 or parts[3]!='attr': print("Please check patch cmd"); sys.exit(2)
        eid=parts[2]; ct=f"+/entities/{H}/+/+/{eid}/#";
        if not check_existence(broker,port,ct,username=username,password=password): print("Error: id doesn't exist"); sys.exit(2)
        with open(file) as jf: data=json.load(jf)
        
        client.on_message=on_message; 
        tp=exists_topic.split('/')[-4]; loc=exists_topic.split('/')[-3]
        if parts[4]=='':
            for k,v in data.items():
                if k not in ('type','id','@context'):
                    st=f"{my_area}/entities/{H}/{tp}/LNA/{eid}/{k}"; client.publish(st,str(v),retain=True,qos=qos)
                    now=str(datetime.datetime.now()); rel={'modifiedAt':[now]}
                    client.publish(f"{my_area}/entities/{H}/{tp}/LNA/{eid}/{k}_timerelsystem_modifiedAt",str(rel['modifiedAt']),retain=True,qos=qos)
        else:
            for k,v in data.items():
                st=f"{my_area}/entities/{H}/{tp}/{loc}/{eid}/{k}"; client.publish(st,str(v),retain=True,qos=qos)
                now=str(datetime.datetime.now()); rel={'modifiedAt':[now]}
                client.publish(f"{my_area}/entities/{H}/{tp}/LNA/{eid}/{k}_timerelsystem_modifiedAt",str(rel['modifiedAt']),retain=True,qos=qos)
        client.loop_stop()

    # GET/entities/
    elif re.search(r"GET/entities/",command):
        context_flag=True
        entity_id_flag=False
        entity_id_pattern_flag=False
        entity_type_flag=False
        entity_attrs_flag=False
        entity_query_flag=False
        context_flag=True
        topic=[]
        area=[]
        typee_multi=[]
        timee=''
        limit=1800
        id='+'
        attrs='#'
        query=''
        geometry=''
        georel=''
        coordinates=''
        geoproperty='location'  #default value for ngsild
        geovar_count=0

        print("Get entity command found")
        client=connect_mqtt(broker,port,username=username,password=password)
        if client is None:
            print("Failed to connect to the broker. Exiting...")
            return

        if HLink=='':
            HLink='+'
            context_flag=False
        else:
            HLink=HLink.replace("/", "§")

        command_parts = command.split("GET/entities/")
        command=command_parts[1]
        if len(command) > 0 and command[0] == "?":
            command=command[1:]
        command_parts = command.split("&")

        for current in command_parts:
            current=current.split("=", 1)
            print(current[0])
            if(current[0]=="id"):
                print("id detected")
                entity_id_flag=True
                id=current[1]
            elif(current[0]=="idPattern"):
                entity_id_pattern_flag=True
                print("id pattern detected")
            elif(current[0]=="type"):
                entity_type_flag=True
                typee_multi=current[1].split(',')
                print("type detected")
            elif(current[0]=="time"):
                timee=current[1]
                print("time detected")
            elif(current[0]=="limit"):
                limit=int(current[1])
                print("pagination limit detected")
            elif(current[0]=="attrs"):
                entity_attrs_flag=True
                attrs=current[1].split(',')
                print("attrs detected")
            elif(current[0]=="q"):
                entity_query_flag=True
                query=current[1]
                print("query detected")
            elif(current[0]=="geoproperty"):
                geoproperty=current[1]
                print("geoproperty detected")
            elif(current[0]=="geometry"):
                geometry=current[1]
                print("geometry detected")
                geovar_count+=1
            elif(current[0]=="georel"):
                georel=current[1]
                print("georel detected")
                geovar_count+=1
            elif(current[0]=="coordinates"):
                coordinates=current[1]
                print("coordinates detected")
                geovar_count+=1
            elif(current[0]=="area"):
                area=current[1].split(',')
            else:
                print("Query not recognised")
                return

        if(geovar_count!=0 and geovar_count!=3):
            print("Incomplete geoquery!")
            return

        if(area==[]):
            area.append('+')

        if(entity_type_flag==False):
            typee_multi=[1]
        if(entity_id_flag==False):
            id="#"

        for typee in typee_multi:
            messages_for_context=[]
            check_top=[]
            if(typee==1):
                typee="#"

            for z in area:
                if(singleidadvertisement==False):
                    check_topic2=f"provider/+/+/{z}/{HLink}/{typee}"
                else:
                    if(typee=="#"):
                        typee="+"
                    check_topic2=f"provider/+/+/{z}/{HLink}/{typee}/{id}"
                check_top.append(check_topic2)

            if(Forwarding==1):
                messages_for_context=GET(broker,port,check_top,0.1,1,username=username,password=password)
            if(typee=="#"):
                typee="+"

            context_providers_addresses=[]
            context_providers_ports=[]
            context_providers_areas=[]
            context_providers_full=[]

            if (Forwarding==1):
                for messg in messages_for_context:
                    initial_topic=(messg.topic).split('/')
                    if initial_topic[1]+initial_topic[2]+initial_topic[3] in context_providers_full:
                        continue
                    context_providers_addresses.append(initial_topic[1])
                    context_providers_ports.append(initial_topic[2])
                    context_providers_areas.append(initial_topic[3])
                    context_providers_full.append(str(initial_topic[1]+initial_topic[2]+initial_topic[3]))
                    top=initial_topic[3]
                    if attrs!='#':
                        for i in attrs:
                            top=f"{initial_topic[3]}/entities/{HLink}/{typee}/+/{id}/{i}"
                            topic.append(top)
                    else:
                        top=f"{initial_topic[3]}/entities/{HLink}/{typee}/+/{id}/#"
                        topic.append(top)
                    messages=GET(initial_topic[1],int(initial_topic[2]),topic,0.5,1,limit)
                    if messages:
                        recreate_multiple_entities(messages,query,attrs,timee=timee,limit=limit,georel=georel,geometry=geometry,coordinates=coordinates,geoproperty=geoproperty,context_given=HLink)
            else:
                print("Forwarding left by default for now")

    elif re.search(r"entityOperations/delete",command):
        # batch delete
        client= connect_mqtt(broker,port,username=username,password=password)
        if client is None:
            print("Failed to connect to the broker. Exiting...")
            return
        with open(file) as jf: json_obj=json.load(jf)
        for eid in json_obj:
            top=f"{my_area}/entities/{(HLink or '+').replace('/','§')}/+/+/{eid}/#"
            if check_existence(broker,port,top,username=username,password=password):
                clear_retained(broker,port,top,username=username,password=password)
                tp=exists_topic.split('/')[-4]
                tc=f"{my_area}/entities/{(HLink or '+').replace('/','§')}/{tp}/#/"
                if not singleidadvertisement:
                    spec=f"provider/{broker}/{port}/{my_area}/{(HLink or '+').replace('/','§')}/{tp}/+"; clear_retained(broker,port,spec,username=username,password=password)
                else:
                    spec=f"provider/{broker}/{port}/{my_area}/{(HLink or '+').replace('/','§')}/{tp}/{eid}"; clear_retained(broker,port,spec,username=username,password=password)

    # entityOperations/create
    elif re.search(r"entityOperations/create",command):
        advertisement_exists={}
    
        client=connect_mqtt(broker,port,username=username,password=password)
        if client is None:  
            print("Failed to connect to the broker. Exiting...")
            return
        client.on_message=on_message;
        with open(file) as jf: json_list=json.load(jf)
        for data in json_list:
            t=data.get('type','')
            bypass=0 if singleidadvertisement or t in advertisement_exists else 0
            post_entity(data,my_area,broker,port,qos,my_loc,bypass,client,username=username,password=password)
            advertisement_exists.setdefault(t,[])

    # entityOperations/update
    elif re.search(r"entityOperations/update",command):
        client=connect_mqtt(broker,port,username=username,password=password)
        client.on_message=on_message; 
        with open(file) as jf: json_list=json.load(jf)
        for data in json_list: post_entity(data,my_area,broker,port,qos,my_loc,1,client,username=username,password=password)

    # entityOperations/upsert
    elif re.search(r"entityOperations/upsert",command):
        client=connect_mqtt(broker,port,username=username,password=password)
        if client is None:
            print("Failed to connect to the broker. Exiting...")
            return
        client.on_message=on_message; 
        with open(file) as jf: json_list=json.lo+ad(jf)
        for data in json_list: post_entity(data,my_area,broker,port,qos,my_loc,1,client,username=username,password=password)

    else:
        print(f"Unknown command: {command}"); usage(); sys.exit(2)

                                                                   
#Check if the script is being run directly
#Retrieve command-line arguments passed to the script
#Call the main function with the command-line arguments

if __name__ == "__main__":
    main(sys.argv[1:])
# ComDeX Action Handler Tool

# Version: 0.6.1
# Author: Nikolaos Papadakis 
# Requirements:
# - Python 3.7 or above
# - Shapely library
# - paho-mqtt library

# For more information and updates, visit: [https://github.com/SAMSGBLab/ComDeX]

import sys
import os
import json
import getopt
import subprocess
import time
import threading
import multiprocessing
import re
import ast
import datetime
import shapely.geometry as shape_geo
import urllib.request
from getpass import getpass
import paho.mqtt.client as mqtt
from pickle import TRUE

#default values of mqtt broker to communicate with
default_broker_address='localhost'
default_broker_port=1026
default_ngsild_context="https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"

#global advertisement flag (to avoid for now passing it in every function)
singleidadvertisement=False

#TO DO convert these globals to nonlocals 
#exists=False
exists_topic=''
full_data=''

allow_anonymous   = True
global_username   = None
global_password   = None

# ── Mosquitto config paths ────────────────────────────────────────────────
MOSQ_CONFIG = os.path.join(os.path.dirname(__file__),
                           'mosquitto', 'config', 'mosquitto.conf')
# print(MOSQ_CONFIG)
PASSWD_FILE = os.path.join(os.path.dirname(__file__),
                           'mosquitto', 'config', 'passwd')
# print(PASSWD_FILE)
ACL_FILE    = os.path.join(os.path.dirname(__file__),
                           'mosquitto', 'config', 'acl')
# print(ACL_FILE)

def reload_mosquitto():
        try:
            # Check if Mosquitto is already running
            pid = subprocess.check_output(["pidof", "-s", "mosquitto"]).decode().strip()
            subprocess.run(["kill", "-HUP", pid], check=True)
            print(f"🔄 Reloaded existing Mosquitto process (PID: {pid})")
        except subprocess.CalledProcessError:
            # Not running, start it with local config
            subprocess.run(["mosquitto", "-c", str(MOSQ_CONFIG), "-d"])
            print("🚀 Started Mosquitto with local config")    

# ── Lock & secure Mosquitto ───────────────────────────────────────────────
def lock_mosquitto():
    global allow_anonymous, global_username, global_password
    allow_anonymous = False

    # 1) Prompt for user/pass
    global_username = input('Enter new MQTT username: ').strip()
    global_password = getpass(f'Enter password for {global_username}: ')

    # 2) Update mosquitto.conf
    conf = []
    with open(MOSQ_CONFIG) as f:
        for line in f:
            if re.match(r'^\s*allow_anonymous\s+', line):
                conf.append('allow_anonymous false\n')
            elif re.match(r'^\s*(password_file|acl_file)\s+', line):
                continue
            else:
                conf.append(line)
    conf += [
        f'password_file ./passwd\n',
        f'acl_file      ./acl\n',
    ]
    with open(MOSQ_CONFIG, 'w') as f:
        f.writelines(conf)

    # 3) Create/update passwd file
    os.makedirs(os.path.dirname(PASSWD_FILE), exist_ok=True)
    # create the passwd file if it doesn't exist
    if not os.path.exists(PASSWD_FILE):
        open(PASSWD_FILE, 'a').close()
    subprocess.run(['mosquitto_passwd', '-b',
                    PASSWD_FILE, global_username, global_password],
                   check=True)

    # 4) Create/update ACL file
    os.makedirs(os.path.dirname(ACL_FILE), exist_ok=True)
    acl = []
    if os.path.exists(ACL_FILE):
        acl = open(ACL_FILE).read().splitlines()
    header = f'user {global_username}'
    if header not in acl:
        acl += [header, 'topic readwrite #']
    with open(ACL_FILE, 'w') as f:
        f.write('\n'.join(acl) + '\n')

    # 5) Reload broker
    reload_mosquitto()
    print('Mosquitto locked down: anonymous disabled, credentials and ACL applied.')

#unlock mosquitto 
def unlock_mosquitto():
    # Re-enable anonymous access and reload config
    global allow_anonymous
    allow_anonymous = True

    # 1) Update mosquitto.conf: set allow_anonymous true
    lines = []
    with open(MOSQ_CONFIG) as f:
        for line in f:
            if line.strip().startswith('allow_anonymous'):
                lines.append('allow_anonymous true\n')
            else:
                lines.append(line)
    with open(MOSQ_CONFIG, 'w') as f:
        f.writelines(lines)

    # 2) Reload broker to apply change
    reload_mosquitto()
    print('Mosquitto unlocked: anonymous access enabled.')




#Function: post_entity
#Description: This function is used to create a new NGSI-LD entity in the ComDeX node.
#Parameters:
#- data: The data of the entity to be created.
#- my_area: The area or domain of the entity.
#- broker: The name or IP address of the broker.
#- port: The port number of the broker.
#- qos: The quality of service level for message delivery.
#- my_loc: The location of the broker (used for advanced advertisements with geoqueries).
#- bypass_existence_check (optional): Flag to bypass the existence check of the entity (default: 0).
#- client (optional): MQTT client object (default: mqtt.Client(clean_session=True)).
# Returns: None
def post_entity(data,my_area,broker,port,qos,my_loc,bypass_existence_check=0,client=mqtt.Client(clean_session=True),username=None, password=None):
    

    global singleidadvertisement

    client.loop_start()     
    if 'type' in data:
        typee=str(data['type'])
    else:
        print("Error, ngsi-ld entity without a type \n")
        sys.exit(2)
    if 'id' in data:  
        id=str(data['id'])
    else:
        print("Error, ngsi-ld entity without a id \n")
        sys.exit(2)
    if '@context' in data:
        if( str(type(data["@context"]))=="<class 'str'>"):
            context=data['@context'].replace("/", "§")
        else:
            context=data['@context'][0].replace("/", "§")

        
        
    else:    
        print("Error, ngsi-ld entity without context \n")
        sys.exit(2)
    if 'location' in data:
        location=data['location'] 
    else:
        location=''       
    
    big_topic=my_area+'/entities/'+context+'/'+typee+'/LNA/'+id     

 
    check_topic='+/entities/'+context+'/'+typee+'/+/'+id+'/#' 
    print("Show me the check topic" + check_topic)
    print("Checking existence of entity...")
    
   
    if(bypass_existence_check==0):
        if (check_existence(broker,port,check_topic,username=username,password=password)!=False):
            print("Error entity with this id already exists, did you mean to patch?")
            return

    #check for remote existance maybe in the future
      
    ################### CREATE SMALL TOPICS!!!!!!!!!!!!!!!#######################
    for key in data.items():
        if key[0]!="type" and key[0]!="id" and key[0]!='@context':
            
            small_topic=my_area+'/entities/'+context+'/'+typee+'/LNA/'+id+'/'+key[0]
            #print(small_topic)
            print("Publishing message to subtopic")    
            
            client.publish(small_topic,str(key[1]),retain=True,qos=qos)
            
            curr_time=str(datetime.datetime.now())
            time_rels = { "createdAt": [curr_time],"modifiedAt": [curr_time] }

            small_topic=my_area+'/entities/'+context+'/'+typee+'/LNA/'+id+'/'+ key[0]+"_timerelsystem_CreatedAt"
                
            client.publish(small_topic,str(time_rels["createdAt"]),retain=True,qos=qos)
            
            small_topic=my_area+'/entities/'+context+'/'+typee+'/LNA/'+id+'/'+ key[0]+"_timerelsystem_modifiedAt"
            client.publish(small_topic,str(time_rels["modifiedAt"]),retain=True,qos=qos)     

    ############################################################################
    check_topic2="provider/+/+/"+my_area+'/'+context+'/'+typee+'/'
    
    if(singleidadvertisement==False):
        special_context_provider_broadcast= 'provider/' + broker + '/' +str(port) + '/'+my_area+'/' + context + '/' +typee
    else:
        special_context_provider_broadcast= 'provider/' + broker + '/' +str(port) + '/'+my_area+'/' + context + '/' +typee +'/'+id
        bypass_existence_check=1
    
    if(bypass_existence_check==1):
        print("Bypassing existence check for advertisement")
        client.publish(special_context_provider_broadcast,"Provider Message: { CreatedAt:" + str(time_rels["createdAt"]) +",location:" + str(my_loc)+"}" ,retain=True,qos=2)
        print(special_context_provider_broadcast) 
    elif(check_existence(broker,port,special_context_provider_broadcast,username=username,password=password)==False):
        print("checking existence of advertisement...")
        info= client.publish(special_context_provider_broadcast,"Provider Message: { CreatedAt:" + str(time_rels["createdAt"]) +",location:" + str(my_loc)+"}" ,retain=True,qos=2)
        # Check the result code
        if info.rc == 0:
            print("Publishing message to provider table")
            print(special_context_provider_broadcast)     
        else:
            print(f"Failed to send publish request to {special_context_provider_broadcast}. Return code: {info.rc}")

          
        #old logging of published messages
        #logger = logging.getLogger()
        #handler = logging.FileHandler('logfile_advertisement_published.log')
        #logger.addHandler(handler)
        #logger.error(time.time_ns()/(10**6))

           
    client.loop_stop()


#Description: This function checks if an entity or advertisement already exists inside the broker.
#Parameters:
#- broker: The name or IP address of the broker.
#- port: The port number of the broker.
#- topic: The topic name or identifier of the message to check.
#Returns:
#- True if the entity/advertisement exists in the broker, False otherwise.

def check_existence(broker,port,topic, username=None, password=None):
    print("checking existence of topic: " + topic + " to the broker: " + broker + " on port: " + str(port) + "using username: " + str(username) + " and password: " + str(password))
    run_flag=TRUE
    exists=False
    expires=1
    def on_connect(client3, userdata, flags, rc):
        print("Connected for existence check with result code "+str(rc))

    # The callback for when a PUBLISH message is received from the server.
    def on_message(client3, userdata, msg):
        global exists_topic
        nonlocal exists
        nonlocal expires
        exists=True
        exists_topic=msg.topic
        expires-=1
    
    client3 = mqtt.Client()   
    client3.username_pw_set(username, password) if username and password else None
    client3.on_connect = on_connect
    client3.on_message = on_message

    
    client3.connect(broker, port)
    client3.loop_start()
    client3.subscribe(topic,qos=1)

    start=time.perf_counter()
    try:
        while run_flag:
            tic_toc=time.perf_counter()
            if (tic_toc-start) > expires:
                run_flag=False
    except:
        pass
    #time.sleep(1)
    client3.loop_stop()  
    #print(exists)
    return exists    


# Function: GET
# Description: This function is used to retrieve entities from the ComDeX node, similar to the NGSI-LD GET entities operation.
# Parameters:
#   - broker: The name or IP address of the broker.
#   - port: The port number of the broker.
#   - topics: A list of topics to subscribe to.
#   - expires: The expiration time in seconds.
#   - qos: The quality of service level for message delivery.
#   - limit (optional): The maximum number of entities to retrieve (default: 2000).
# Returns:
#   - A list of received messages (entities) ordered via their id.

def GET(broker, port, topics, expires, qos, limit=2000,username=None, password=None):
    run_flag = True
    messagez = []
    messages_by_id = {}

    # The callback for when a PUBLISH message is received from the server.
    def on_message(client, userdata, msg):
        nonlocal messagez
        nonlocal expires
        nonlocal messages_by_id
        nonlocal limit
        if msg.retain == 1:
            initial_topic = msg.topic.split('/')
            id = initial_topic[-2]
            messages_by_id.setdefault(id, []).append(msg)
            if len(messages_by_id) == limit + 1:
                expires -= 10000000
            else:
                messagez.append(msg)
                expires += 0.5

    # Create an MQTT client
    client = mqtt.Client()
    client.username_pw_set(username, password) if username and password else None
    client.on_message = on_message
    client.connect(broker, port)

    client.loop_start()

    # Subscribe to the specified topics
    for topic in topics:
        client.subscribe(topic, qos)

    start = time.perf_counter()

    try:
        while run_flag:
            tic_toc = time.perf_counter()
            if tic_toc - start > expires:
                run_flag = False
    except:
        pass

    client.loop_stop()

    # Return the received messages (entities)
    return messagez


# Function: recreate_single_entity
# Description: This function recreates a single entity from the received messages based on the specified query conditions.
# This is possible because each entity has a unique message id, which is used as the catalyst for the entity reconstruction from
# its various attribute messages
# Parameters:
#   - messagez: List of received messages (entities).
#   - query: Query condition to filter the entities (optional, default: '').
#   - topics: Topic filters to apply (optional, default: '').
#   - timee: Time condition to filter the entities (optional, default: '').
#   - georel: Geo-relation condition to filter the entities (optional, default: '').
#   - geometry: Geometry type for the geospatial condition (optional, default: '').
#   - coordinates: Coordinates for the geospatial condition (optional, default: '').
#   - geoproperty: Geospatial property for the geospatial condition (optional, default: '').
#   - context_given: Context value for entity comparison (optional, default: '').
# Returns: None

def recreate_single_entity(messagez, query='', topics='', timee='', georel='', geometry='', coordinates='', geoproperty='', context_given=''):
    query_flag_passed = False
    subqueries_flags = {}
    default_context = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"

    # Extract initial topic information
    initial_topic = (messagez[0].topic).split('/')
    id = initial_topic[-2]
    typee = initial_topic[-4]
    context = initial_topic[-5]
    context = context.replace("§", "/")
    context_text = context
    contextt = []
    contextt.append(context_text)

    # Add default context if it differs from the specified context
    if context_text != default_context:
        contextt.append(default_context)

    # Initialize data dictionary with ID and type
    data = {}
    data['id'] = id
    data['type'] = typee

    # Check if a specific context is given for comparison
    if context_given == '+':
        with urllib.request.urlopen(context_text) as url:
            data_from_web = json.loads(url.read().decode())
        try:
            data['type'] = data_from_web["@context"][typee]
        except:
            dummy_command = "This is a dummy command for except"

    if query == '':
        for msg in messagez:
            attr_str = msg.payload
            attr_str = attr_str.decode(encoding='UTF-8', errors='strict')
            attr_str = attr_str.replace("\'", "\"")
            data2 = json.loads(attr_str)
            topic = (msg.topic).split('/')

            # Check geospatial condition if specified
            if georel != '':
                if topic[-1] == geoproperty:
                    geo_type = str(data2["value"]["type"])
                    geo_coord = str(data2["value"]["coordinates"])
                    geo_ok = 0

                    geo_type = geo_type.replace(" ", "")
                    geo_coord = geo_coord.replace(" ", "")
                    coordinates = coordinates.replace(" ", "")

                    geo_entity = shape_geo.shape((data2["value"]))

                    if geometry == "Point":
                        query_gjson = shape_geo.Point(json.loads(coordinates))
                    elif geometry == "LineString":
                        query_gjson = shape_geo.LineString(json.loads(coordinates))
                    elif geometry == "Polygon":
                        query_gjson = shape_geo.Polygon(json.loads(coordinates))
                    elif geometry == "MultiPoint":
                        query_gjson = shape_geo.MultiPoint(json.loads(coordinates))
                    elif geometry == "MultiLineString":
                        query_gjson = shape_geo.MultiLineString(json.loads(coordinates))
                    elif geometry == "MultiPolygon":
                        query_gjson = shape_geo.MultiPolygon(json.loads(coordinates))

                    # Check specific georelation condition
                    if georel == "equals":
                        if geo_entity.equals(query_gjson):
                            geo_ok = 1
                        else:
                            return
                    elif georel == "within":
                        if geo_entity.within(query_gjson):
                            geo_ok = 1
                        else:
                            return
                    elif georel == "intersects":
                        if geo_entity.intersects(query_gjson):
                            geo_ok = 1
                        else:
                            return
                    elif re.search("near;", georel):
                        near_query = georel.split(';')
                        near_operator = re.findall('[><]|==|>=|<=', near_query[1])
                        near_geo_queries = (re.split('[><]|==|>=|<=', near_query[1]))

                        if str(near_geo_queries[0]) == "maxDistance":
                            if str(near_operator[0]) == "==":
                                if geo_entity.distance(query_gjson) > float(near_geo_queries[1]):
                                    return
                        elif str(near_geo_queries[0]) == "minDistance":
                            if str(near_operator[0]) == "==":
                                if geo_entity.distance(query_gjson) < float(near_geo_queries[1]):
                                    return
                    elif georel == "contains":
                        if geo_entity.contains(query_gjson):
                            geo_ok = 1
                        else:
                            return
                    elif georel == "disjoint":
                        if geo_entity.disjoint(query_gjson):
                            geo_ok = 1
                        else:
                            return
                    elif georel == "overlaps":
                        if geo_entity.overlaps(query_gjson):
                            geo_ok = 1
                        else:
                            return

            # Check topic filters if specified
            if topics != '' and topics != "#":
                if topic[-1] in topics:
                    data[topic[-1]] = data2
                if topic[-1].endswith("_CreatedAt") or topic[-1].endswith("_modifiedAt"):
                    if timee != '':
                        time_topic = (topic[-1].split('_timerelsystem_'))
                        if context_given == '+':
                            try:
                                time_topic[-2] = data_from_web["@context"][time_topic[-2]]
                            except:
                                dummy_command = "This is a dummy command for except"

                        data[time_topic[-2]][time_topic[-1]] = data2

            else:
                if topic[-1].endswith("_CreatedAt") or topic[-1].endswith("_modifiedAt"):
                    if timee != '':
                        time_topic = (topic[-1].split('_timerelsystem_'))
                        if context_given == '+':
                            try:
                                time_topic[-2] = data_from_web["@context"][time_topic[-2]]
                            except:
                                dummy_command = "This is a dummy command for except"

                        data[time_topic[-2]][time_topic[-1]] = data2
                else:
                    data[topic[-1]] = data2

        data['@context'] = contextt

        json_data = json.dumps(data, indent=4, ensure_ascii=False)
        print(json_data)

    elif query != '':
        logical_operators = re.findall('[;|()]', query)
        queries_big = re.split(('[;|()]'), query)

        for msg in messagez:
            attr_str = msg.payload
            attr_str = attr_str.decode(encoding='UTF-8', errors='strict')
            attr_str = attr_str.replace("\'", "\"")
            data2 = json.loads(attr_str)
            topic = (msg.topic).split('/')

            # Check geospatial condition if specified
            if georel != '':
                if topic[-1] == geoproperty:
                    geo_type = str(data2["value"]["type"])
                    geo_coord = str(data2["value"]["coordinates"])
                    geo_ok = 0

                    geo_type = geo_type.replace(" ", "")
                    geo_coord = geo_coord.replace(" ", "")
                    coordinates = coordinates.replace(" ", "")

                    geo_entity = shape_geo.shape((data2["value"]))

                    if geometry == "Point":
                        query_gjson = shape_geo.Point(json.loads(coordinates))
                    elif geometry == "LineString":
                        query_gjson = shape_geo.LineString(json.loads(coordinates))
                    elif geometry == "Polygon":
                        query_gjson = shape_geo.Polygon(json.loads(coordinates))
                    elif geometry == "MultiPoint":
                        query_gjson = shape_geo.MultiPoint(json.loads(coordinates))
                    elif geometry == "MultiLineString":
                        query_gjson = shape_geo.MultiLineString(json.loads(coordinates))
                    elif geometry == "MultiPolygon":
                        query_gjson = shape_geo.MultiPolygon(json.loads(coordinates))

                    # Check specific georelation condition
                    if georel == "equals":
                        if geo_entity.equals(query_gjson):
                            geo_ok = 1
                        else:
                            return
                    elif georel == "within":
                        if geo_entity.within(query_gjson):
                            geo_ok = 1
                        else:
                            return
                    elif georel == "intersects":
                        if geo_entity.intersects(query_gjson):
                            geo_ok = 1
                        else:
                            return
                    elif re.search("near;", georel):
                        near_query = georel.split(';')
                        near_operator = re.findall('[><]|==|>=|<=', near_query[1])
                        near_geo_queries = (re.split('[><]|==|>=|<=', near_query[1]))

                        if str(near_geo_queries[0]) == "maxDistance":
                            if str(near_operator[0]) == "==":
                                if geo_entity.distance(query_gjson) > float(near_geo_queries[1]):
                                    return
                        elif str(near_geo_queries[0]) == "minDistance":
                            if str(near_operator[0]) == "==":
                                if geo_entity.distance(query_gjson) < float(near_geo_queries[1]):
                                    return
                    elif georel == "contains":
                        if geo_entity.contains(query_gjson):
                            geo_ok = 1
                        else:
                            return
                    elif georel == "disjoint":
                        if geo_entity.disjoint(query_gjson):
                            geo_ok = 1
                        else:
                            return
                    elif georel == "overlaps":
                        if geo_entity.overlaps(query_gjson):
                            geo_ok = 1
                        else:
                            return

            # Allowing combination of logical queries
            for query2 in queries_big:
                operator = re.findall('[><]|==|>=|<=', query2)
                queries = (re.split('[><]|==|>=|<=', query2))
                subqueries_flags.setdefault(queries[0], False)

                if queries[0] == topic[-1]:

                    if str(operator[0]) == "==":

                        if isinstance(data2["value"], list):
                            for data3 in data2["value"]:
                                if data3 == queries[1]:
                                    subqueries_flags[queries[0]] = True

                        elif data2["value"] == queries[1]:
                            subqueries_flags[queries[0]] = True
                    elif queries[1].isnumeric():
                        if str(operator[0]) == ">":
                            if float(data2["value"]) > float(queries[1]):
                                subqueries_flags[queries[0]] = True
                        elif str(operator[0]) == "<":
                            if float(data2["value"]) < float(queries[1]):
                                subqueries_flags[queries[0]] = True
                        elif str(operator[0]) == "<=":
                            if float(data2["value"]) <= float(queries[1]):
                                subqueries_flags[queries[0]] = True
                        elif str(operator[0]) == ">=":
                            if float(data2["value"]) >= float(queries[1]):
                                subqueries_flags[queries[0]] = True

            # Check topic filters if specified
            if topics != '' and topics != "#":
                if topic[-1] in topics:
                    data[topic[-1]] = data2
                if topic[-1].endswith("_CreatedAt") or topic[-1].endswith("_modifiedAt"):
                    if timee != '':
                        time_topic = (topic[-1].split('_timerelsystem_'))
                        if context_given == '+':
                            try:
                                time_topic[-2] = data_from_web["@context"][time_topic[-2]]
                            except:
                                dummy_command = "This is a dummy command for except"

                        data[time_topic[-2]][time_topic[-1]] = data2

            else:
                if topic[-1].endswith("_CreatedAt") or topic[-1].endswith("_modifiedAt"):
                    if timee != '':
                        time_topic = (topic[-1].split('_timerelsystem_'))
                        if context_given == '+':
                            try:
                                time_topic[-2] = data_from_web["@context"][time_topic[-2]]
                            except:
                                dummy_command = "This is a dummy command for except"

                        data[time_topic[-2]][time_topic[-1]] = data2
                else:
                    data[topic[-1]] = data2

        data['@context'] = contextt

        l = 0
        full_logical_equation = []
        subqueries_flags.pop('', None)
        for results in subqueries_flags.values():
            full_logical_equation.append(str(results))
            if l < (len(logical_operators)):
                if logical_operators[l] != '':
                    if logical_operators[l] == ";":
                        full_logical_equation.append('and')
                    elif logical_operators[l] == "|":
                        full_logical_equation.append('or')
                    else:
                        full_logical_equation.append(logical_operators[l])
                    if l + 1 < (len(logical_operators) - 1):
                        while logical_operators[l + 1] != ';' and logical_operators[l + 1] != '|':
                            l = l + 1
                            full_logical_equation.append(logical_operators[l])

            l = l + 1

        query_flag_passed = eval(' '.join(full_logical_equation))
        if query_flag_passed == True:
            json_data = json.dumps(data, indent=4, ensure_ascii=False)
            print(json_data)
        
                 



        
# Function: recreate_multiple_entities
# Description: This function recreates multiple entities from the received messages based on the specified query conditions. It basically calls the 
# recreate single entity command, over a list of MQTT messages, based on their id.
# Parameters:
#   - messagez: List of received messages (entities).
#   - query: Query condition to filter the entities (optional, default: '').
#   - topics: Topic filters to apply (optional, default: '').
#   - timee: Time condition to filter the entities (optional, default: '').
#   - limit: Maximum number of entities to recreate (optional, default: 2000).
#   - georel: Geo-relation condition to filter the entities (optional, default: '').
#   - geometry: Geometry type for the geospatial condition (optional, default: '').
#   - coordinates: Coordinates for the geospatial condition (optional, default: '').
#   - geoproperty: Geospatial property for the geospatial condition (optional, default: '').
#   - context_given: Context value for entity comparison (optional, default: '').
# Returns: None

def recreate_multiple_entities(messagez, query='', topics='', timee='', limit=2000, georel='', geometry='', coordinates='', geoproperty='', context_given=''):
    messages_by_id = {}

    # Separate each message by ID to recreate using the recreate_single_entity function
    for message in messagez:
        initial_topic = (message.topic).split('/')
        id = initial_topic[-2]
        messages_by_id.setdefault(id, []).append(message)

    # Iterate over single entities and recreate them using recreate_single_entity function
    for single_entities in messages_by_id.values():
        recreate_single_entity(single_entities, query, topics, timee, georel, geometry, coordinates, geoproperty, context_given)

        # Countdown pagination limit
        limit = limit - 1
        if limit == 0:
            break




        

# Function: multiple_subscriptions
# Description: This function sets up multiple subscriptions based on the specified flags and parameters.
# Parameters:
#   - entity_type_flag: Flag indicating whether entity type is specified.
#   - watched_attributes_flag: Flag indicating whether watched attributes are specified.
#   - entity_id_flag: Flag indicating whether entity ID is specified.
#   - area: Area value for the subscriptions.
#   - context: Context value for the subscriptions.
#   - truetype: Entity type value for the subscriptions.
#   - true_id: Entity ID value for the subscriptions.
#   - expires: Expiration time for the subscriptions.
#   - broker: MQTT broker address.
#   - port: MQTT broker port.
#   - qos: Quality of Service level for the subscriptions.
#   - watched_attributes: List of watched attributes for the subscriptions.
# Returns: None

def multiple_subscriptions(entity_type_flag, watched_attributes_flag, entity_id_flag, area, context, truetype, true_id, expires, broker, port, qos, watched_attributes):
    topic = []
    
    if entity_type_flag and watched_attributes_flag and entity_id_flag:
        # Subscribe to topics based on entity type, watched attributes, and entity ID
        for attr in watched_attributes:
            topic.append(area + '/entities/' + context + '/' + truetype + '/+/' + true_id + '/' + attr)
    
    elif entity_type_flag and entity_id_flag:
        # Subscribe to topics based on entity type and entity ID
        topic.append(area + '/entities/' + context + '/' + truetype + '/+/' + true_id + '/#')
    
    elif watched_attributes_flag and entity_id_flag:
        # Subscribe to topics based on watched attributes and entity ID
        for attr in watched_attributes:
            topic.append(area + '/entities/' + context + '/+/+/' + true_id + '/' + attr)
    
    elif entity_type_flag and watched_attributes_flag:
        # Subscribe to topics based on entity type and watched attributes
        for attr in watched_attributes:
            topic.append(area + '/entities/' + context + '/' + truetype + '/+/+/' + attr)
    
    elif entity_type_flag:
        # Subscribe to topics based on entity type
        topic.append(area + '/entities/' + context + '/' + truetype + '/#')
    
    elif entity_id_flag:
        # Subscribe to topics based on entity ID
        topic.append(area + '/entities/' + context + '/+/+/' + true_id + '/#')
    
    elif watched_attributes_flag:
        # Subscribe to topics based on watched attributes
        for attr in watched_attributes:
            topic.append(area + '/entities/' + context + '/+/+/+/' + attr)
    
    else:
        print("Something has gone wrong, program did not find the topics to subscribe to!")
        sys.exit(2)
    
    # Call the subscribe function with the generated topics
    subscribe(broker, port, topic, expires, qos, context_given=context)
    



# Function: subscribe
# Description: This function subscribes to MQTT topics and handles the received messages.
# Parameters:
#   - broker: MQTT broker address.
#   - port: MQTT broker port.
#   - topics: List of topics to subscribe to.
#   - expires: Expiration time for the subscriptions.
#   - qos: Quality of Service level for the subscriptions.
#   - context_given: Context value for entity comparison.
# Returns: None

def subscribe(broker, port, topics, expires, qos, context_given):
    run_flag = True
    
    def read_credentials(broker_address, port):
        try:
            with open('passwd_mapping.txt', 'r') as f:
                for line in f:
                    parts = line.strip().split()
                    if len(parts) != 3:
                        continue
                    addr_port, username, password = parts
                    addr, prt = addr_port.split(':')
                    if addr == broker_address and int(prt) == port:
                        return username, password
        except FileNotFoundError:
            print("Error: passwd_mapping.txt file not found.")
        return None, None

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected successfully.")
        else:
            print(f"Connection failed with return code {rc}")

    def connect_with_logic(broker_address, port):
        client = mqtt.Client() 

        username, password = read_credentials(broker_address, port)

        if username and password:
            client.username_pw_set(username, password)
            print(f"Trying to connect to {broker_address}:{port} with credentials from file...")
            try:
                client.connect(broker_address, port)
                client.loop_start()
                return client
            except Exception as e:
                print(f"Error: Connection with credentials failed. Details: {e}")
                return None
        else:
            print(f"No credentials found in file for {broker_address}:{port}. Trying anonymous connection...")
            try:
                client.connect(broker_address, port)
                client.loop_start()
                return client
            except Exception as e:
                print(f"Anonymous connection also failed. Please provide credentials in passwd_mapping.txt.")
                return None



    # # The callback for when the client receives a CONNACK response from the server.
    # def on_connect(client, userdata, flags, rc):
    #     print("Connected with result code " + str(rc))

    # The callback for when a PUBLISH message is received from the server.
    def on_message(client, userdata, msg):
        # Perform operations on received message
        if msg.payload.decode() != '':
            stamp = time.time_ns() / (10 ** 6)
            initial_topic = (msg.topic).split('/')
            ids = initial_topic[-2]
            tmp = msg.payload
            attr_str = msg.payload.decode(encoding='UTF-8', errors='strict')
            attr_str = attr_str.replace("\'", "\"")
            data2 = json.loads(attr_str)

            #Older logging messages for experiments
            # Logging the message arrival time difference
            #handler = logging.FileHandler('logfile_mqtt_notification_arrived.log')
            # logger.addHandler(handler)
            # logger.error(str(stamp - float(data2["value"])))

        if msg.payload.decode() != '':
            messagez = []
            messagez.append(msg)
            if msg.topic.endswith("_CreatedAt") or msg.topic.endswith("_modifiedAt"):
                # Do nothing for special system time topics (ignore them, no temporal subscriptions)
                do_nothing = 1
            else:
                # Recreate single entity based on the received message
                recreate_single_entity(messagez, timee=0, context_given=context_given)
        else:
            print("\n Message on topic:" + msg.topic + ", was deleted")

    client = connect_with_logic(broker, port)
    if client is None:
        print("Failed to connect to the broker. Exiting...")
        return
    
    client.on_message = on_message

    # Connect to MQTT broker
    client.connect(broker, port, keepalive=expires)

    client.loop_start()
    for topic in topics:
        client.subscribe(topic, qos)
        #print("Subscribing to topic: " +topic)

    start = time.perf_counter()

    try:
        while run_flag:
            tic_toc = time.perf_counter()
            if (tic_toc - start) > expires:
                run_flag = False
    except:
        pass

    print("Subscriptions expired, exiting.....")
    # Blocking call that processes network traffic, dispatches callbacks and handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a manual interface.
    client.loop_stop()

# Function: subscribe_for_advertisement_notification
# Description: This function sets up subscriptions for advertisement notifications based on the specified flags and parameters, basically to
# find if a new advertisement of interest arrives while an intersted subscriber is active (so that the subscriber while also connect to the
# new advertised source "on the fly").
# Parameters:
#   - broker: MQTT broker address.
#   - port: MQTT broker port.
#   - topics: List of topics to subscribe to for advertisement notifications.
#   - expires: Expiration time for the subscriptions.
#   - qos: Quality of Service level for the subscriptions.
#   - entity_type_flag: Flag indicating whether entity type is specified.
#   - watched_attributes_flag: Flag indicating whether watched attributes are specified.
#   - entity_id_flag: Flag indicating whether entity ID is specified.
#   - watched_attributes: List of watched attributes for the subscriptions.
#   - true_id: Entity ID value for the subscriptions.
# Returns: None

def subscribe_for_advertisement_notification(broker, port, topics, expires, qos, entity_type_flag, watched_attributes_flag, entity_id_flag, watched_attributes, true_id,username=None, password=None):
    run_flag = True
    advertisement_exists = {}
    jobs_to_terminate = {}
    print(topics)
    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(client, userdata, flags, rc):
        print(f"Connecting to MQTT broker at {broker}:{port} {'with credentials' if username else 'anonymously'}")
        print("Connected for advertisement notification with result code " + str(rc))
        if rc == 5:
            print("Authentication failed: invalid username or password.")
            client.loop_stop()
            raise ConnectionError("MQTT authentication failed with result code 5 (Not authorized)")
        elif rc != 0:
            print(f"Connection failed with result code {rc}")
            client.loop_stop()
            raise ConnectionError(f"MQTT connection failed with code {rc}")

    # The callback for when a PUBLISH message is received from the server.
    def on_message(client, userdata, msg):
        print("MESSAGE advert HERE")
        print(msg.payload.decode())
        if msg.payload.decode() != '':
            # Calculate at which time an interested subscriber received an advertisement message
            # that did not exist prior to its subscription
            nonlocal advertisement_exists

            stamp = time.time_ns() / (10 ** 6)

            initial_topic = (msg.topic).split('/')
            broker_remote = initial_topic[1]
            port_remote = int(initial_topic[2])
            area_remote = initial_topic[3]
            context = initial_topic[4]
            truetype = initial_topic[5]

            topic2 = "provider/" + broker_remote + '/' + str(port_remote) + '/' + area_remote + '/' + context + '/' + truetype
            attr_str = topic2
            print(attr_str)

            if topic2 in advertisement_exists.keys():
                print("advertisement_already_exists")
                return ()
            else:
                print("found_brand_new_advertisement")
                advertisement_exists.setdefault(topic2, [])

            topic = []

            context_providers_addresses = []
            context_providers_ports = []
            context_providers_areas = []
            number_of_threads = 1

            context_providers_addresses.append(initial_topic[1])
            context_providers_ports.append(initial_topic[2])
            context_providers_areas.append(initial_topic[3])

            nonlocal jobs_to_terminate
            jobs = []
            for i in range(0, number_of_threads):
                #print("How many threads???")
                process = multiprocessing.Process(
                    target=multiple_subscriptions,
                    args=(
                        entity_type_flag, watched_attributes_flag, entity_id_flag,
                        context_providers_areas[i], context, truetype, true_id, expires,
                        context_providers_addresses[i], int(context_providers_ports[i]), qos, watched_attributes
                    )
                )
                jobs.append(process)

            print(jobs)
            jobs_to_terminate.setdefault(topic2, jobs)
            jobs_to_terminate[topic2] = jobs
            for j in jobs:
                print(j)
                j.start()

        else:
            initial_topic = (msg.topic).split('/')
            broker_remote = initial_topic[1]
            port_remote = int(initial_topic[2])
            area_remote = initial_topic[3]
            context = initial_topic[4]
            truetype = initial_topic[5]

            topic2 = "provider/" + broker_remote + '/' + str(port_remote) + '/' + area_remote + '/' + context + '/' + truetype
            print("Advertisement Deleted")
            advertisement_exists.pop(topic2, 1)
            print(jobs_to_terminate)
            for j in jobs_to_terminate[topic2]:
                print(j)
                j.kill()

    client = mqtt.Client()
    print("username and password")
    print(username, password)
    client.username_pw_set(username, password) if username and password else None
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker, port, keepalive=expires)

    client.loop_start()
    for topic in topics:
        client.subscribe(topic, qos)
        print("Subscribing to topic: " + topic)

    start = time.perf_counter()

    try:
        while run_flag:
            tic_toc = time.perf_counter()
            if (tic_toc - start) > expires:
                run_flag = False
    except:
        pass

    print("Subscriptions expired, exiting.....")
    client.loop_stop()


# Function: clear_retained
# Description: This function clears the retained messages on the specified topic(s).
# Parameters:
#   - broker: MQTT broker address.
#   - port: MQTT broker port.
#   - retained: Single topic or list of topics to clear retained messages from.
# Returns: None

def clear_retained(broker, port, retained,username=None, password=None):
    run_flag = True
    expires = 0.5

    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(client, userdata, flags, rc):
        print("Connected with result code " + str(rc))

    # The callback for when a PUBLISH message is received from the server.
    def on_message(client, userdata, msg):
        nonlocal expires

        if (msg.retain == 1):
            # Publish a null message to clear the retained message
            client2.publish(msg.topic, None, 0, True)
            print("Clearing retained on topic -", msg.topic)
            expires += 0.1

    client = mqtt.Client()
    client2 = mqtt.Client()
    client.username_pw_set(username, password) if username and password else None
    client2.username_pw_set(username, password) if username and password else None
    client.on_connect = on_connect
    client2.on_connect = on_connect
    client.on_message = on_message

    client.connect(broker, port)
    client2.connect(broker, port)
    client.loop_start()
    client2.loop_start()
    client.subscribe(retained, qos=1)

    start = time.perf_counter()
    try:
        while run_flag:
            tic_toc = time.perf_counter()
            if (tic_toc - start) > expires:
                run_flag = False
    except:
        pass

    client.loop_stop()
    client2.loop_stop()


#debug functions to see mqtt broker communication
############
# Function: on_message
# Description: Callback function called when a message is received from the MQTT broker.
# Parameters:
#   - client: MQTT client instance.
#   - userdata: Custom userdata provided by the client.
#   - message: MQTT message object containing the received message.
# Returns: None

def on_message(client, userdata, message):
    # Print the received message payload, topic, QoS, and retain flag
    print("message received ", str(message.payload.decode("utf-8")))
    print("message topic=", message.topic)
    print("message qos=", message.qos)
    print("message retain flag=", message.retain)

# Function: on_log
# Description: Callback function called when a log message is generated by the MQTT client.
# Parameters:
#   - client: MQTT client instance.
#   - userdata: Custom userdata provided by the client.
#   - level: Log level of the message.
#   - buf: Log message string.
# Returns: None

def on_log(client, userdata, level, buf):
    # Print the log message
    print("log: ", buf)
#########################################   




def usage():
    print("\nUsage:")
    print("python actionhandler.py [options]\n")
    
    print("Options:")
    print("-h, --help                Show this help message and exit")
    print("  -L, --lock                Lock and secure Mosquitto (disable anonymous, set credentials)")
    print("  -U, --unlock   Re-enable anonymous and reload Mosquitto")
    print("-c, --command             Provide the command, possible commands include [POST/entities,POST/Subscriptions,DELETE/entities/,PATCH/entities/,GET/entities/,entityOperations/delete,entityOperations/create,entityOperations/update,entityOperations/upsert]")
    print("-f, --file                Specify the file to be used as input to the command")
    print("-b, --broker_address      Specify the address of the MQTT broker of the ComDeX node")
    print("-p, --port                Specify the port number of the MQTT broker of the ComDeX node")
    print("-q, --qos                 Specify the Quality of Service level (0, 1, or 2) to be used for the specified command")
    print("-H, --HLink               Specify the HLink, 'context link' to be used for the GET request")
    print("-A, --singleidadvertisement Specify if single ID advertisement is to be used (use 1 for True), default is false")
    
    print("\nExample:")
    print("python3 actionhandler.py -c POST/entities -f entity.ngsild -b localhost -p 1026 -q 1 -H HLink -A 0\n")




#ComDeX is an ngsild compliant "broker" that utilises a running MQTT broker
#Here is the main function where different functions are called mainly based on the selected by the user command.

def main(argv):

    try:
        opts, args = getopt.getopt(argv,"hc:f:b:p:l:q:H:A:K:U:N:S",["command=","file=","broker_address=","port=","qos=","HLink=","singleidadvertisement=","lock=","unlock=","username=","password="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    # Initialize defaults
    command = ''
    file = ''
    HLink = ''
    qos = 0
    Forwarding = 1
    broker = default_broker_address
    port = default_broker_port
    expires = 3600
    global singleidadvertisement
    singleidadvertisement = False
    my_area = "unknown_area"
    my_loc = "unknown_location"
    lock_flag = False
    unlock_flag = False
    username = None
    password = None
    # Parse ComDeX flags
    for opt, arg in opts:
        if opt == '-h':
            usage(); sys.exit()
        elif opt in ("-c", "--command"):
            command = arg
        elif opt in ("-f", "--file"):
            file = arg
        elif opt in ("-b", "--broker_address"):
            broker = arg
        elif opt in ("-p", "--port") or opt == '-l':
            port = int(arg)
        elif opt in ("-q", "--qos"):
            qos = int(arg)
            if qos < 0 or qos > 2:
                print("Invalid Mqtt qos"); sys.exit(2)
        elif opt in ("-H", "--HLink"):
            HLink = arg
        elif opt in ("-A", "--singleidadvertisement"):
            singleidadvertisement = (arg == "1")
        elif opt in ("-K","--lock"):
            lock_flag = True
        elif opt in ("-U","--unlock"):
            unlock_flag = True
        elif opt in ("-N","--username"):
            username = arg
        elif opt in ("-S","--password"):
            password = arg
    # print(lock_flag,unlock_flag)        
    if lock_flag:
            lock_mosquitto()
            print("Mosquitto is now locked")
    if unlock_flag:
            unlock_mosquitto()
            print("Mosquitto is now unlocked")
            
    # Broker location awareness
    open("broker_location_awareness.txt", "a+").close()
    with open("broker_location_awareness.txt", "r") as f:
        contents = f.read()
    try:
        location_awareness = ast.literal_eval(contents)
        area_key = f"{broker}:{port}:area"
        loc_key  = f"{broker}:{port}:loc"
        if area_key in location_awareness:
            my_area = location_awareness[area_key]
        if loc_key in location_awareness:
            my_loc = location_awareness[loc_key]
    except:
        pass

    if not command:
        print("No command found, exiting..."); sys.exit(2)
    
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected successfully.")
        else:
            print(f"Connection failed with return code {rc}")
   
    def connect_mqtt(broker_address, port, username=None, password=None):
        client = mqtt.Client(clean_session=True)
        client.on_connect = on_connect

        if username and password:
            client.username_pw_set(username, password)
            print(f"Trying to connect to {broker_address}:{port} with credentials...")
            try:
                client.connect(broker_address, port)
                client.loop_start()
                return client
            except Exception as e:
                print("Provide correct username and password.")
                return None
        else:
            print(f"Trying anonymous connection to {broker_address}:{port}...")
            try:
                client.connect(broker_address, port)
                client.loop_start()
                return client
            except Exception as e:
                print("Anonymous connection failed. Provide username and password.")
                return None

    # POST ENTITIES
    if command == "POST/entities":
        print("creating new instance")
        client = connect_mqtt(broker, port, username=username, password=password)
        client.on_message = on_message
        if not file:
            usage(); sys.exit(2)

        print("NGSI-LD POST entity command detected")
        with open(file) as jf:
            try:
                data = json.load(jf)
            except json.JSONDecodeError:
                print("Can't parse the input file, are you sure it is valid JSON?")
                sys.exit(2)
        if client is None:
            print("Failed to connect to the broker. Exiting...")
            return
        post_entity(data, my_area, broker, port, qos, my_loc, 0, client,username=username,password=password)

    # CREATION OF SUBSCRIPTIONS
    elif command == 'POST/Subscriptions':
        
        truetype=''; true_id=''; entity_type_flag=False; watched_attributes_flag=False; entity_id_flag=False; watched_attributes=''
        if not file: usage(); sys.exit(2)
        print("ngsild Post Subscription command detected")
        with open(file) as jf:
            try: data=json.load(jf)
            except: print("Can't parse the input file, are you sure it is valid json?"); sys.exit(2)
        if data.get('type')!='Subscription': print(f"Subscription has invalid type: {data.get('type')}"); sys.exit(2)
        sid = str(data['id']) if 'id' in data else (print("Error, ngsi-ld Subscription without a id "), sys.exit(2))[0]
        ctx = data.get('@context');
        if isinstance(ctx,str): context=ctx.replace('/','§')
        else: context=ctx[0].replace('/','§') if ctx else '+'
        if 'entities' in data:
            ie=data['entities'][0]
            if 'type' in ie: truetype=str(ie['type']); entity_type_flag=True
            if 'id' in ie:   true_id=str(ie['id']);   entity_id_flag=True
        if 'watchedAttributes' in data:
            watched_attributes=data['watchedAttributes']; watched_attributes_flag=True
            if watched_attributes is None: print("Watched attributes without content, exiting...."); sys.exit(2)
        expires=int(data.get('expires',expires))
        if not (entity_type_flag or watched_attributes_flag or entity_id_flag): print("Error, ngsi-ld subscription without information about topics, exiting.... "); sys.exit(2)
        big_topic=f"{my_area}/Subscriptions/{context}/Subscription/LNA/{sid}"

        client1=connect_mqtt(broker,port,username=username,password=password)
        if client1 is None:
            print("Failed to connect to the broker. Exiting...")
            return
        client1.on_message=on_message; 
        client1.publish(big_topic,str(data),qos=qos); client1.loop_stop()
        area=data.get('area',['+']); truetype2=truetype or '#'; trueid2=true_id or '#'; check_top=[]
        for z in area:
            if not singleidadvertisement: check_top.append(f"provider/+/+/{z}/{context}/{truetype2}")
            else:                      check_top.append(f"provider/+/+/{z}/{context}/{truetype}/{trueid2}")
        subscribe_for_advertisement_notification(broker,port,check_top,expires,qos,entity_type_flag,watched_attributes_flag,entity_id_flag,watched_attributes,true_id,username=username,password=password)

    # DELETE/entities/
    elif re.search(r"DELETE/entities/",command):
        username =username
        password = password
        client=connect_mqtt(broker,port,username=username,password=password)
        if client is None:
            print("Failed to connect to the broker. Exiting...")
            return
        parts=command.split('/')
        if len(parts)<5:
            H=HLink.replace('/','§') if HLink else '+'; eid=parts[2]
            top=f"{my_area}/entities/{H}/+/+/{eid}/#"
            if not check_existence(broker,port,top,username=username,password=password ): print("Entity with this id doesn't exist, no need for deletion"); sys.exit(2)
            clear_retained(broker,port,top,username=username,password=password)
            tp=exists_topic.split('/')[-4]; tc=f"{my_area}/entities/{H}/{tp}/+/+/#"
            if not singleidadvertisement:
                spec=f"provider/{broker}/{port}/{my_area}/{H}/{tp}"
                if not check_existence(broker,port,tc,username=username,password=password): clear_retained(broker,port,spec,username=username,password=password)
            else: spec=f"provider/{broker}/{port}/{my_area}/{H}/{tp}/{eid}"; clear_retained(broker,port,spec,username=username,password=password)
        else:
            H=HLink.replace('/','§') if HLink else '+'
            if parts[3]!='attrs': print("Please check delete attr cmd"); sys.exit(2)
            eid=parts[2]; top=f"{my_area}/entities/{H}/+/+/{eid}/{parts[4]}"; clear_retained(broker,port,top,username=username,password=password)

    # PATCH/entities/
    elif re.search(r"PATCH/entities/",command):
        client=connect_mqtt(broker,port,username=username,password=password)
        if client is None:
            print("Failed to connect to the broker. Exiting...")
            return
        
        H=HLink.replace('/','§') if HLink else '+'; parts=command.split('/')
        if len(parts)<5 or parts[3]!='attr': print("Please check patch cmd"); sys.exit(2)
        eid=parts[2]; ct=f"+/entities/{H}/+/+/{eid}/#";
        if not check_existence(broker,port,ct,username=username,password=password): print("Error: id doesn't exist"); sys.exit(2)
        with open(file) as jf: data=json.load(jf)
        
        client.on_message=on_message; 
        tp=exists_topic.split('/')[-4]; loc=exists_topic.split('/')[-3]
        if parts[4]=='':
            for k,v in data.items():
                if k not in ('type','id','@context'):
                    st=f"{my_area}/entities/{H}/{tp}/LNA/{eid}/{k}"; client.publish(st,str(v),retain=True,qos=qos)
                    now=str(datetime.datetime.now()); rel={'modifiedAt':[now]}
                    client.publish(f"{my_area}/entities/{H}/{tp}/LNA/{eid}/{k}_timerelsystem_modifiedAt",str(rel['modifiedAt']),retain=True,qos=qos)
        else:
            for k,v in data.items():
                st=f"{my_area}/entities/{H}/{tp}/{loc}/{eid}/{k}"; client.publish(st,str(v),retain=True,qos=qos)
                now=str(datetime.datetime.now()); rel={'modifiedAt':[now]}
                client.publish(f"{my_area}/entities/{H}/{tp}/LNA/{eid}/{k}_timerelsystem_modifiedAt",str(rel['modifiedAt']),retain=True,qos=qos)
        client.loop_stop()

    # GET/entities/
    elif re.search(r"GET/entities/",command):
        context_flag=True
        entity_id_flag=False
        entity_id_pattern_flag=False
        entity_type_flag=False
        entity_attrs_flag=False
        entity_query_flag=False
        context_flag=True
        topic=[]
        area=[]
        typee_multi=[]
        timee=''
        limit=1800
        id='+'
        attrs='#'
        query=''
        geometry=''
        georel=''
        coordinates=''
        geoproperty='location'  #default value for ngsild
        geovar_count=0

        print("Get entity command found")
        client=connect_mqtt(broker,port,username=username,password=password)
        if client is None:
            print("Failed to connect to the broker. Exiting...")
            return

        if HLink=='':
            HLink='+'
            context_flag=False
        else:
            HLink=HLink.replace("/", "§")

        command_parts = command.split("GET/entities/")
        command=command_parts[1]
        if len(command) > 0 and command[0] == "?":
            command=command[1:]
        command_parts = command.split("&")

        for current in command_parts:
            current=current.split("=", 1)
            print(current[0])
            if(current[0]=="id"):
                print("id detected")
                entity_id_flag=True
                id=current[1]
            elif(current[0]=="idPattern"):
                entity_id_pattern_flag=True
                print("id pattern detected")
            elif(current[0]=="type"):
                entity_type_flag=True
                typee_multi=current[1].split(',')
                print("type detected")
            elif(current[0]=="time"):
                timee=current[1]
                print("time detected")
            elif(current[0]=="limit"):
                limit=int(current[1])
                print("pagination limit detected")
            elif(current[0]=="attrs"):
                entity_attrs_flag=True
                attrs=current[1].split(',')
                print("attrs detected")
            elif(current[0]=="q"):
                entity_query_flag=True
                query=current[1]
                print("query detected")
            elif(current[0]=="geoproperty"):
                geoproperty=current[1]
                print("geoproperty detected")
            elif(current[0]=="geometry"):
                geometry=current[1]
                print("geometry detected")
                geovar_count+=1
            elif(current[0]=="georel"):
                georel=current[1]
                print("georel detected")
                geovar_count+=1
            elif(current[0]=="coordinates"):
                coordinates=current[1]
                print("coordinates detected")
                geovar_count+=1
            elif(current[0]=="area"):
                area=current[1].split(',')
            else:
                print("Query not recognised")
                return

        if(geovar_count!=0 and geovar_count!=3):
            print("Incomplete geoquery!")
            return

        if(area==[]):
            area.append('+')

        if(entity_type_flag==False):
            typee_multi=[1]
        if(entity_id_flag==False):
            id="#"

        for typee in typee_multi:
            messages_for_context=[]
            check_top=[]
            if(typee==1):
                typee="#"

            for z in area:
                if(singleidadvertisement==False):
                    check_topic2=f"provider/+/+/{z}/{HLink}/{typee}"
                else:
                    if(typee=="#"):
                        typee="+"
                    check_topic2=f"provider/+/+/{z}/{HLink}/{typee}/{id}"
                check_top.append(check_topic2)

            if(Forwarding==1):
                messages_for_context=GET(broker,port,check_top,0.1,1,username=username,password=password)
            if(typee=="#"):
                typee="+"

            context_providers_addresses=[]
            context_providers_ports=[]
            context_providers_areas=[]
            context_providers_full=[]

            if (Forwarding==1):
                for messg in messages_for_context:
                    initial_topic=(messg.topic).split('/')
                    if initial_topic[1]+initial_topic[2]+initial_topic[3] in context_providers_full:
                        continue
                    context_providers_addresses.append(initial_topic[1])
                    context_providers_ports.append(initial_topic[2])
                    context_providers_areas.append(initial_topic[3])
                    context_providers_full.append(str(initial_topic[1]+initial_topic[2]+initial_topic[3]))
                    top=initial_topic[3]
                    if attrs!='#':
                        for i in attrs:
                            top=f"{initial_topic[3]}/entities/{HLink}/{typee}/+/{id}/{i}"
                            topic.append(top)
                    else:
                        top=f"{initial_topic[3]}/entities/{HLink}/{typee}/+/{id}/#"
                        topic.append(top)
                    messages=GET(initial_topic[1],int(initial_topic[2]),topic,0.5,1,limit)
                    if messages:
                        recreate_multiple_entities(messages,query,attrs,timee=timee,limit=limit,georel=georel,geometry=geometry,coordinates=coordinates,geoproperty=geoproperty,context_given=HLink)
            else:
                print("Forwarding left by default for now")

    elif re.search(r"entityOperations/delete",command):
        # batch delete
        client= connect_mqtt(broker,port,username=username,password=password)
        if client is None:
            print("Failed to connect to the broker. Exiting...")
            return
        with open(file) as jf: json_obj=json.load(jf)
        for eid in json_obj:
            top=f"{my_area}/entities/{(HLink or '+').replace('/','§')}/+/+/{eid}/#"
            if check_existence(broker,port,top,username=username,password=password):
                clear_retained(broker,port,top,username=username,password=password)
                tp=exists_topic.split('/')[-4]
                tc=f"{my_area}/entities/{(HLink or '+').replace('/','§')}/{tp}/#/"
                if not singleidadvertisement:
                    spec=f"provider/{broker}/{port}/{my_area}/{(HLink or '+').replace('/','§')}/{tp}/+"; clear_retained(broker,port,spec,username=username,password=password)
                else:
                    spec=f"provider/{broker}/{port}/{my_area}/{(HLink or '+').replace('/','§')}/{tp}/{eid}"; clear_retained(broker,port,spec,username=username,password=password)

    # entityOperations/create
    elif re.search(r"entityOperations/create",command):
        advertisement_exists={}
    
        client=connect_mqtt(broker,port,username=username,password=password)
        if client is None:  
            print("Failed to connect to the broker. Exiting...")
            return
        client.on_message=on_message;
        with open(file) as jf: json_list=json.load(jf)
        for data in json_list:
            t=data.get('type','')
            bypass=0 if singleidadvertisement or t in advertisement_exists else 0
            post_entity(data,my_area,broker,port,qos,my_loc,bypass,client,username=username,password=password)
            advertisement_exists.setdefault(t,[])

    # entityOperations/update
    elif re.search(r"entityOperations/update",command):
        client=connect_mqtt(broker,port,username=username,password=password)
        client.on_message=on_message; 
        with open(file) as jf: json_list=json.load(jf)
        for data in json_list: post_entity(data,my_area,broker,port,qos,my_loc,1,client,username=username,password=password)

    # entityOperations/upsert
    elif re.search(r"entityOperations/upsert",command):
        client=connect_mqtt(broker,port,username=username,password=password)
        if client is None:
            print("Failed to connect to the broker. Exiting...")
            return
        client.on_message=on_message; 
        with open(file) as jf: json_list=json.lo+ad(jf)
        for data in json_list: post_entity(data,my_area,broker,port,qos,my_loc,1,client,username=username,password=password)

    else:
        print(f"Unknown command: {command}"); usage(); sys.exit(2)

                                                                   
#Check if the script is being run directly
#Retrieve command-line arguments passed to the script
#Call the main function with the command-line arguments

if __name__ == "__main__":
    main(sys.argv[1:])
