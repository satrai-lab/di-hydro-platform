import nvdlib, time, json, uuid, random
import pandas as pd

MITIGATION_FILE = "data/NIST/cwe_mitigation.csv"

def getCweMitigation(cveid) :
    df = pd.read_csv(MITIGATION_FILE, sep=';')
    df = df[df['cve'] == cveid][["cve","phase","strategy"]]
    return df.to_dict('records')

def generate_vuln_files():
    ### Electric Vehicle
    vuln_vehicle=[]
    vulnid_vehicle=[]
    cve_vehicle = nvdlib.searchCVE(keywordSearch='Vehicle Service Management System')
    for cve in cve_vehicle: 
        vulnid_vehicle.append(cve.id)
        vuln_vehicle.append(cve)
    
    with open("data/NIST/vehicle.json", "w") as outfile:
        json_data = json.dumps({"vulnerabilities":vuln_vehicle
                    },default=lambda o: o.__dict__, indent=2)
        outfile.write(json_data)

    ### Charging stations
    vuln_charging=[]
    vulnid_charging=[]
    cve_charging = nvdlib.searchCVE(keywordSearch='evlink v3.4.0.1')
    for cve in cve_charging: 
        vulnid_charging.append(cve.id)
        vuln_charging.append(cve)
    
    with open("data/NIST/charging.json", "w") as outfile:
        json_data = json.dumps({"vulnerabilities":vuln_charging
                    },default=lambda o: o.__dict__, indent=2)
        outfile.write(json_data)

    time.sleep(6)
    ### MQTT
    vuln_mqtt=[]
    vulnid_mqtt=[]
    cve_mqtt = nvdlib.searchCVE(keywordSearch='mqtt')
    for cve in cve_mqtt: 
        vulnid_mqtt.append(cve.id)
        vuln_mqtt.append(cve)

    with open("data/NIST/broker.json", "w") as outfile:
        json_data = json.dumps({"vulnerabilities":vuln_mqtt
                    },default=lambda o: o.__dict__, indent=2)
        outfile.write(json_data)

    time.sleep(6)
    ### Redis
    vuln_redis=[]
    vulnid_redis=[]
    cve_redis = nvdlib.searchCVE(keywordSearch='redis 6.2.6')
    for cve in cve_redis: 
        vulnid_redis.append(cve.id)
        vuln_redis.append(cve)

    time.sleep(6)
    ### Django
    vuln_django=[]
    vulnid_django=[]
    cve_django = nvdlib.searchCVE(keywordSearch='django 3.2')
    for cve in cve_django: 
        vulnid_django.append(cve.id)
        vuln_django.append(cve)

    with open("data/NIST/management.json", "w") as outfile:
        json_data = json.dumps({"vulnerabilities":vuln_redis+vuln_django
                    },default=lambda o: o.__dict__, indent=2)
        outfile.write(json_data)

    time.sleep(6)
    ### Postgres
    vuln_postgres=[]
    vulnid_postgres=[]
    cve_postgres = nvdlib.searchCVE(keywordSearch='postgresql 15.5')
    for cve in cve_postgres: 
        vulnid_postgres.append(cve.id)
        vuln_postgres.append(cve)
    
    time.sleep(6)
    ### Elasticsearch
    vuln_elastic=[]
    vulnid_elastic=[]
    cve_elastic = nvdlib.searchCVE(keywordSearch='elasticsearch 7.17')
    for cve in cve_elastic: 
        vulnid_elastic.append(cve.id)
        vuln_elastic.append(cve)
    
    time.sleep(6)
    ### filebrowser
    vuln_file=[]
    vulnid_file=[]
    cve_file = nvdlib.searchCVE(keywordSearch='filebrowser 2.22')
    for cve in cve_file: 
        vulnid_file.append(cve.id)
        vuln_file.append(cve)

    with open("data/NIST/storage.json", "w") as outfile:
        json_data = json.dumps({"vulnerabilities":vuln_elastic+vuln_file+vuln_postgres
                    },default=lambda o: o.__dict__, indent=2)
        outfile.write(json_data)

    return

def build_v2x_net(num_charging=1,num_management=1,num_storage=1,num_vehicle=1):
    vulnid_mqtt=[]
    mitig_mqtt=[]
    with open("data/NIST/broker.json") as f: vuln_broker = json.load(f)["vulnerabilities"]
    vuln_broker_filter = random.sample(vuln_broker,10)
    for v in vuln_broker_filter: 
        vulnid_mqtt.append(v["id"])
        mitig_mqtt+=getCweMitigation(v["id"])
    
    # broker_hosts=[]
    # for i in range(0,num_broker):
    #     broker_hosts.append({
    #         'id': str(uuid.uuid4()),
    #         'hostname':"Broker",
    #         'community': "broker",
    #         'network_interfaces':[{
    #             'ipaddress':"192.168.0.1",
    #             'macaddress':"ad:49:52:ba:19:76",
    #             'ports':[{
    #                 "number": 1883,
    #                 "state": "open",
    #                 "protocol": "MQTT",
    #                 "services": [{
    #                     "name": "pubsub",
    #                     "cve_list": vulnid_mqtt
    #                 }]
    #             }]
    #         }]
    #     })

    
    vulnid_charging=[]
    mitig_charging=[]
    with open("data/NIST/charging.json") as f: vuln_charging = json.load(f)["vulnerabilities"]
    for v in vuln_charging: 
        vulnid_charging.append(v["id"])
        mitig_charging+=getCweMitigation(v["id"])

    ### Charging stations
    charging_hosts=[]
    charging_hostsSOA=[]
    for i in range(0,num_charging):
        iddev=str(uuid.uuid4())
        
        charging_hosts.append({
            'id': iddev,
            'hostname':"Charging Station",
            'community': "charging",
            'network_interfaces':[{
                'ipaddress':"192.168.0.1",
                'macaddress':"ad:49:52:ba:19:76",
                'ports':[{
                    "number": 1883,
                    "state": "open",
                    "protocol": "MQTT",
                    "services": [{
                        "name": "pubsub",
                        "cve_list": vulnid_charging+vulnid_mqtt
                    }]
                }]
            }]
        })

        charging_hostsSOA.append({
            'id': iddev,
            'hostname':"Charging Station",
            'community': "charging",
            'network_interfaces':[{
                'ipaddress':"192.168.0.1",
                'macaddress':"ad:49:52:ba:19:76",
                'ports':[{
                    "number": 1883,
                    "state": "open",
                    "protocol": "MQTT",
                    "services": [{
                        "name": "pubsub",
                        "cve_list": vulnid_charging
                    }]
                }]
            }]
        })
   

    vulnid_mng=[]
    mitig_mng=[]
    with open("data/NIST/management.json") as f: vuln_mng = json.load(f)["vulnerabilities"]
    for v in vuln_mng: 
        vulnid_mng.append(v["id"])
        mitig_mng+=getCweMitigation(v["id"])

    management_hosts=[]
    management_hostsSOA=[]
    for i in range(0,num_management):
        iddev=str(uuid.uuid4())
        management_hosts.append({
            'id': iddev,
            'hostname':"Management Platform",
            'community': "management",
            'network_interfaces':[{
                'ipaddress':"192.168.0.1",
                'macaddress':"ad:49:52:ba:19:76",
                'ports':[{
                    "number": 8080,
                    "state": "open",
                    "protocol": "TCP",
                    "services": [{
                        "name": "tcp",
                        "cve_list": vulnid_mng+vulnid_mqtt
                    }]
                }]
            }]
        })
        management_hostsSOA.append({
            'id': iddev,
            'hostname':"Management Platform",
            'community': "management",
            'network_interfaces':[{
                'ipaddress':"192.168.0.1",
                'macaddress':"ad:49:52:ba:19:76",
                'ports':[{
                    "number": 8080,
                    "state": "open",
                    "protocol": "TCP",
                    "services": [{
                        "name": "tcp",
                        "cve_list": vulnid_mng
                    }]
                }]
            }]
        })

    vulnid_store=[]
    mitig_store=[]
    with open("data/NIST/storage.json") as f: vuln_store = json.load(f)["vulnerabilities"]
    for v in vuln_store: 
        vulnid_store.append(v["id"])
        mitig_store+=getCweMitigation(v["id"])
    
    storage_hosts=[]
    storage_hostsSOA=[]
    for i in range(0,num_storage):
        iddev=str(uuid.uuid4())
        storage_hosts.append({
            'id': iddev,
            'hostname':"Storage Device",
            'community': "vehicle",
            'network_interfaces':[{
                'ipaddress':"192.168.0.1",
                'macaddress':"ad:49:52:ba:19:76",
                'ports':[{
                    "number": 8080,
                    "state": "open",
                    "protocol": "TCP",
                    "services": [{
                        "name": "tcp",
                        "cve_list": vulnid_store+vulnid_mqtt
                    }]
                }]
            }]
        })
        storage_hostsSOA.append({
            'id': iddev,
            'hostname':"Storage Device",
            'community': "vehicle",
            'network_interfaces':[{
                'ipaddress':"192.168.0.1",
                'macaddress':"ad:49:52:ba:19:76",
                'ports':[{
                    "number": 8080,
                    "state": "open",
                    "protocol": "TCP",
                    "services": [{
                        "name": "tcp",
                        "cve_list": vulnid_store
                    }]
                }]
            }]
        })

    vulnid_vehicle=[]
    mitig_vehicle=[]
    with open("data/NIST/vehicle.json") as f: vuln_vehicle = json.load(f)["vulnerabilities"]
    for v in vuln_vehicle: 
        vulnid_vehicle.append(v["id"])
        mitig_vehicle+=getCweMitigation(v["id"])

    ### Vehicles
    vehicle_hosts=[]
    vehicle_hostsSOA=[]
    for i in range(0,num_vehicle):
        iddev=str(uuid.uuid4())
        
        vehicle_hosts.append({
            'id': iddev,
            'hostname':"Electric Vehicle",
            'community': "vehicle",
            'network_interfaces':[{
                'ipaddress':"192.168.0.1",
                'macaddress':"ad:49:52:ba:19:76",
                'ports':[{
                    "number": 1883,
                    "state": "open",
                    "protocol": "MQTT",
                    "services": [{
                        "name": "pubsub",
                        "cve_list": vulnid_vehicle+vulnid_mqtt
                    }]
                }]
            }]
        })

        vehicle_hostsSOA.append({
            'id': iddev,
            'hostname':"Electric Vehicle",
            'community': "vehicle",
            'network_interfaces':[{
                'ipaddress':"192.168.0.1",
                'macaddress':"ad:49:52:ba:19:76",
                'ports':[{
                    "number": 1883,
                    "state": "open",
                    "protocol": "MQTT",
                    "services": [{
                        "name": "pubsub",
                        "cve_list": vulnid_vehicle
                    }]
                }]
            }]
        })

    edges=[]
    for h1 in management_hosts:
        h1id=h1["id"]
        for h2 in management_hosts:
            h2id=h2["id"]
            if h1id!=h2id:
                edges.append([h1id,h2id])
                edges.append([h2id,h1id])

    for h1 in storage_hosts:
        h1id=h1["id"]
        for h2 in storage_hosts:
            h2id=h2["id"]
            if h1id!=h2id:
                edges.append([h1id,h2id])
                edges.append([h2id,h1id])

    for hcharg in charging_hosts:
        hcid=hcharg["id"]
        # for hstore in storage_hosts:
        #     hstid=hstore["id"]
        #     edges.append([hcid,hstid])
        for hmng in management_hosts:
            hmngid=hmng["id"]
            edges.append([hcid,hmngid])
            edges.append([hmngid,hcid])

    for hveh in vehicle_hosts:
        hcid=hveh["id"]
        # for hstore in storage_hosts:
        #     hstid=hstore["id"]
        #     edges.append([hcid,hstid])
        for hmng in management_hosts:
            hmngid=hmng["id"]
            edges.append([hcid,hmngid])
            edges.append([hmngid,hcid])
    
    for hstore in storage_hosts:
        hstid=hstore["id"]
        for hmng in management_hosts:
            hmngid=hmng["id"]
            edges.append([hstid,hmngid])
            # edges.append([hmngid,hstid])

    with open("data/v2x_network.json", "w") as outfile:
        json_data = json.dumps({
            "devices": charging_hosts+management_hosts+storage_hosts,
            "vulnerabilities":vuln_charging+vuln_broker_filter+vuln_mng+vuln_store,
            "edges":edges,
            "mitigations":mitig_charging+mitig_mqtt+mitig_mng+mitig_store
        },default=lambda o: o.__dict__, indent=2)
        outfile.write(json_data)

    with open("data/v2x_networkSOA.json", "w") as outfile:
        json_data = json.dumps({
            "devices": charging_hostsSOA+management_hostsSOA+storage_hostsSOA,
            "vulnerabilities":vuln_charging+vuln_mng+vuln_store,
            "edges":edges,
            "mitigations":mitig_charging+mitig_mng+mitig_store
        },default=lambda o: o.__dict__, indent=2)
        outfile.write(json_data)    

if __name__ == "__main__":
    # generate_vuln_files()
    build_v2x_net(10,7,7,5)