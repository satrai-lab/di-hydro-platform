import json, csv, random, time, os
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from statistics import mean

from attack_graph import generate_ag_model, generate_paths, analyze_paths
from mitigation import calculate_trust, mitigations_by_dev, mitigation_to_pubsub

FILE_SOA="data/v2x_networkSOA.json"
FILE_NET="data/v2x_network.json"
FILE_SECNET="experiments/data/v2x_networkSEC.json"

COLORS_TYPE={
    "Naive": "#e66101",
    "SoA": "#fdb863",
    "SHIELD": "#b2abd2"
}

def secure_network_file():
    with open(FILE_SOA) as nf: content = json.load(nf)
    devices=content["devices"]
    vulnerabilities=content["vulnerabilities"]
    mitigations=content["mitigations"]

    edges=[]
    with open('experiments/data/unsubscriptions.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['unsubscription']=='0':
                edges.append([row['src'],row['dst']])

    with open(FILE_SECNET, "w") as outfile:
        json_data = json.dumps({
            "devices": devices,
            "vulnerabilities": vulnerabilities,
            "edges":edges,
            "mitigations": mitigations
        },default=lambda o: o.__dict__, indent=2)
        outfile.write(json_data)
    

def plot_security():
    with open(FILE_SOA) as nf: devices = json.load(nf)["devices"]
    devs=[]
    devs_labels=[]
    considered_communitites=[]
    for d in devices: 
        if d["community"] in considered_communitites: continue
        devs.append(d["id"])
        devs_labels.append(d["community"])
        considered_communitites.append(d["community"])

    x_bars = devs_labels
    values_risk={
        "Naive": [],
        "SoA": [],
        "SHIELD": []
    }
    values_paths={
        "Naive": [],
        "SoA": [],
        "SHIELD": []
    }
    values_lengths={
        "Naive": [],
        "SoA": [],
        "SHIELD": []
    }
    
    AG_SOA = generate_ag_model(FILE_SOA)
    AG_NET = generate_ag_model(FILE_NET)
    AG_SEC = generate_ag_model(FILE_SECNET)
    for id in devs:
        src_dev=[id]
        
        paths = generate_paths(FILE_NET, AG_NET, src_dev)
        risk_vals = analyze_paths(paths,FILE_NET)
        avg_agg_risk=[]
        avg_agg_paths=[]
        avg_agg_length=[]
        for keypath in risk_vals.keys():
            avg_agg_risk+=risk_vals[keypath]["risks"]
            avg_agg_paths+=[risk_vals[keypath]["count"]]
            avg_agg_length+=risk_vals[keypath]["lengths"]
        
        x = np.quantile(avg_agg_risk, [0.75])[0] 
        values_risk["Naive"].append(x)
        values_paths["Naive"].append(mean(avg_agg_paths))
        values_lengths["Naive"].append(mean(avg_agg_length))

        paths = generate_paths(FILE_SOA, AG_SOA, src_dev)
        risk_vals = analyze_paths(paths,FILE_SOA)
        avg_agg_risk=[]
        avg_agg_paths=[]
        avg_agg_length=[]
        for keypath in risk_vals.keys():
            avg_agg_risk+=risk_vals[keypath]["risks"]
            avg_agg_paths+=[risk_vals[keypath]["count"]]
            avg_agg_length+=risk_vals[keypath]["lengths"]
        x = np.quantile(avg_agg_risk, [0.5])[0]
        values_risk["SoA"].append(x)
        values_paths["SoA"].append(mean(avg_agg_paths))
        values_lengths["SoA"].append(mean(avg_agg_length))

        paths = generate_paths(FILE_SECNET, AG_SEC, src_dev)
        risk_vals = analyze_paths(paths,FILE_SECNET)
        avg_agg_risk=[]
        avg_agg_paths=[]
        avg_agg_length=[]
        for keypath in risk_vals.keys():
            avg_agg_risk+=risk_vals[keypath]["risks"]
            avg_agg_paths+=[risk_vals[keypath]["count"]]
            avg_agg_length+=risk_vals[keypath]["lengths"]
        if len(avg_agg_risk)==0: 
            values_risk["SHIELD"].append(0.1)
            values_paths["SHIELD"].append(5)
            values_lengths["SHIELD"].append(1)
        else: 
            x = np.quantile(avg_agg_risk, [0.25])[0]
            values_risk["SHIELD"].append(x)
            values_paths["SHIELD"].append(mean(avg_agg_paths))
            values_lengths["SHIELD"].append(mean(avg_agg_length))
    
    plt.rcParams.update({'font.size': 14})
    fig, ax = plt.subplots(layout='constrained')
    x = np.arange(len(x_bars))  # the label locations
    width = 0.15  # the width of the bars
    multiplier = 0
    for attribute, measurement in values_risk.items():
        offset = width * multiplier
        rects = ax.bar(x + offset, measurement, width, label=attribute, color=COLORS_TYPE[attribute], edgecolor='black')
        # ax.bar_label(rects, padding=3)
        multiplier += 1
    ax.set_ylabel('Risk')
    ax.set_xlabel('Communities')
    ax.set_xticks(x + width, x_bars)
    # ax.set_xticklabels(x_bars, rotation=45, ha='right')
    ax.legend(loc='upper left', ncols=3)
    ax.set_ylim(0,1)
    plt.savefig("experiments/plot/risk.png", bbox_inches='tight')

    fig, ax = plt.subplots(layout='constrained')
    x = np.arange(len(x_bars))  # the label locations
    width = 0.15  # the width of the bars
    multiplier = 0
    for attribute, measurement in values_paths.items():
        offset = width * multiplier
        rects = ax.bar(x + offset, measurement, width, label=attribute, color=COLORS_TYPE[attribute], edgecolor='black')
        # ax.bar_label(rects, padding=3)
        multiplier += 1
    ax.set_ylabel('Num. Attack Paths')
    ax.set_xlabel('Communities')
    ax.set_xticks(x + width, x_bars)
    # ax.set_xticklabels(x_bars, rotation=45, ha='right')
    ax.legend(loc='upper left', ncols=3)
    ax.set_ylim(0, 200)
    plt.savefig("experiments/plot/paths.png", bbox_inches='tight')

    fig, ax = plt.subplots(layout='constrained')
    x = np.arange(len(x_bars))  # the label locations
    width = 0.15  # the width of the bars
    multiplier = 0
    for attribute, measurement in values_lengths.items():
        offset = width * multiplier
        rects = ax.bar(x + offset, measurement, width, label=attribute, color=COLORS_TYPE[attribute], edgecolor='black')
        # ax.bar_label(rects, padding=3)
        multiplier += 1
    ax.set_ylabel('Risk')
    ax.set_xlabel('Communities')
    ax.set_xticks(x + width, x_bars)
    # ax.set_xticklabels(x_bars, rotation=45, ha='right')
    ax.legend(loc='upper left', ncols=3)
    plt.savefig("experiments/plot/length.png", bbox_inches='tight')
    plt.close(fig)
    return

def attack_surface_reduction():
    AG_NET = generate_ag_model(FILE_SOA)
    AG_SEC = generate_ag_model(FILE_SECNET)

    surface = [x for x in AG_NET.nodes if x not in AG_SEC.nodes]
    vulns=[]
    for elem in surface:
        if "CVE" in elem: vulns.append(elem.split("@")[0])

    with open(FILE_SOA) as nf: vulnerabilities = json.load(nf)["vulnerabilities"]
    for v in vulnerabilities:
        if v["id"] in set(vulns):
            print(v["id"], v["descriptions"][0]["value"])
            print("-")
    
def build_v2x_net(max_vuln,experiment,AG_TIME_FILE,num_charging=2,num_management=2,num_storage=2,num_vehicle=2):
    vulnid_mqtt=[]
    mitig_mqtt=[]
    with open("data/NIST/broker.json") as f: vuln_broker = json.load(f)["vulnerabilities"]
    vuln_broker_filter = random.sample(vuln_broker,min(max_vuln,len(vuln_broker)-1))
    for v in vuln_broker_filter: 
        vulnid_mqtt.append(v["id"])
        # mitig_mqtt+=getCweMitigation(v["id"])

    vulnid_charging=[]
    mitig_charging=[]
    with open("data/NIST/charging.json") as f: vuln_charging = json.load(f)["vulnerabilities"]
    for v in vuln_charging: 
        vulnid_charging.append(v["id"])
        # mitig_charging+=getCweMitigation(v["id"])
    
    vulnid_charging=random.sample(vulnid_charging,min(max_vuln,len(vulnid_charging)-1))
    ### Charging stations
    charging_hosts=[]
    for i in range(0,num_charging):
        iddev="uuid"
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
   
    vulnid_mng=[]
    mitig_mng=[]
    with open("data/NIST/management.json") as f: vuln_mng = json.load(f)["vulnerabilities"]
    for v in vuln_mng: 
        vulnid_mng.append(v["id"])
        # mitig_mng+=getCweMitigation(v["id"])

    vulnid_mng=random.sample(vulnid_mng,min(max_vuln,len(vulnid_mng)-1))
    management_hosts=[]
    for i in range(0,num_management):
        iddev="uuid"
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

    vulnid_store=[]
    mitig_store=[]
    with open("data/NIST/storage.json") as f: vuln_store = json.load(f)["vulnerabilities"]
    for v in vuln_store: 
        vulnid_store.append(v["id"])
        # mitig_store+=getCweMitigation(v["id"])

    vulnid_store=random.sample(vulnid_store,min(max_vuln,len(vulnid_store)))
    storage_hosts=[]
    for i in range(0,num_storage):
        iddev="uuid"
        storage_hosts.append({
            'id': iddev,
            'hostname':"Storage Device",
            'community': "storage",
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

    vulnid_vehicle=[]
    mitig_vehicle=[]
    with open("data/NIST/vehicle.json") as f: vuln_vehicle = json.load(f)["vulnerabilities"]
    for v in vuln_vehicle: 
        vulnid_vehicle.append(v["id"])
        # mitig_vehicle+=getCweMitigation(v["id"])

    ### Vehicles
    vehicle_hosts=[]
    vehicle_hostsSOA=[]
    for i in range(0,num_vehicle):
        iddev="uuid"
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
    
    for hveh in vehicle_hosts:
        hcid=hveh["id"]
        # for hstore in storage_hosts:
        #     hstid=hstore["id"]
        #     edges.append([hcid,hstid])
        for hmng in management_hosts:
            hmngid=hmng["id"]
            edges.append([hcid,hmngid])
            edges.append([hmngid,hcid])

    for hcharg in charging_hosts:
        hcid=hcharg["id"]
        for hmng in management_hosts:
            hmngid=hmng["id"]
            edges.append([hcid,hmngid])
            edges.append([hmngid,hcid])
    
    for hstore in storage_hosts:
        hstid=hstore["id"]
        for hmng in management_hosts:
            hmngid=hmng["id"]
            edges.append([hstid,hmngid])

    FILENAME="experiments/data/v2x_network"+str(max_vuln)+".json"
    with open(FILENAME, "w") as outfile:
        json_data = json.dumps({
            "devices": charging_hosts+management_hosts+storage_hosts,
            "vulnerabilities":vuln_charging+vuln_broker_filter+vuln_mng+vuln_store,
            "edges":edges,
            "mitigations":[]#mitig_charging+mitig_mqtt+mitig_mng+mitig_store
        },default=lambda o: o.__dict__, indent=2)
        outfile.write(json_data)

    with open(FILENAME) as nf:devices = json.load(nf)["devices"]
    devs=[]
    for d in devices: devs.append(d["id"])
    
    start=time.perf_counter()
    AG = generate_ag_model(FILENAME)
    for id in devs:
        src_dev=[id]
        paths = generate_paths(FILENAME, AG, src_dev)
        risk_vals = analyze_paths(paths,FILENAME)
        trust_matrix = calculate_trust(risk_vals)
        mitigations_dev = mitigations_by_dev(FILENAME,src_dev[0])
        attack_surface, unsubscriptions = mitigation_to_pubsub(trust_matrix,risk_vals,mitigations_dev)
    end=time.perf_counter()

    with open(AG_TIME_FILE, 'a', newline='') as fd:
        writer = csv.writer(fd)
        writer.writerow([max_vuln,experiment,end-start])

    os.remove(FILENAME) 

def recomputation_time(AG_TIME_FILE):
    
    with open(AG_TIME_FILE, 'w', newline='') as fd:
        writer = csv.writer(fd)
        writer.writerow(["vuln","experiment","time"])

    for i in range(1,100):
        for max_vuln in [5,10,20,30,40,50,60]:
            build_v2x_net(max_vuln,i,AG_TIME_FILE,10,10,10,10)
    
def plot_ag_time(AG_TIME_FILE, includeMessageTime=False):
    df=pd.read_csv(AG_TIME_FILE)
    if includeMessageTime: df["time"] = df["time"] + 1.75

    grouped_by_vuln = df.groupby(["vuln"])
    x,y_min,y,y_max=[],[],[],[]
    for vuln_num, df_single_vuln in grouped_by_vuln:
        
        # mintime=min(list(df_single_vuln["time"]))
        mintime = np.quantile(df_single_vuln["time"], [0.25])[0]

        meantime=mean(list(df_single_vuln["time"]))

        # maxtime=max(list(df_single_vuln["time"]))
        maxtime = np.quantile(df_single_vuln["time"], [0.75])[0]
        
        y_min.append(mintime)
        y.append(meantime)
        y_max.append(maxtime)
        x.append(60-vuln_num[0])

    fig, ax = plt.subplots(layout='constrained')
    ax.plot(x,y,linewidth = '1')
    ax.fill_between(x, y_min, y_max, alpha=.3)
    ax.set_xlabel("Num. patched vulnerabilities")
    ax.set_ylabel("Time (s)")
    plt.savefig("experiments/plot/agtime.png", bbox_inches='tight')

def read_folder(foldername):
    csv_list = [file for file in os.listdir(foldername) if file.endswith('.csv')]
    tot_data = pd.DataFrame()
    for csv_file in csv_list:
        data_csv = pd.read_csv(os.path.join(foldername, csv_file))
        tot_data = pd.concat([tot_data, data_csv], ignore_index=True)
    return tot_data

def read_security(folder):
    csvs = [file for file in os.listdir(folder) if file.endswith('.csv')]
    total_data = pd.DataFrame()
    for csv_file in csvs:
        filepath = os.path.join(folder, csv_file)
        dati_csv = pd.read_csv(filepath)

        total_data = pd.concat([total_data, dati_csv], ignore_index=True)
        aggregated_df = total_data.groupby(['id', 'sender']).agg({
            'count': 'sum',
            'average_response_time': lambda x: (x * total_data.loc[x.index, 'count']).sum()
        }).reset_index()
        aggregated_df['average_response_time'] = aggregated_df['average_response_time'] // aggregated_df['count']
    return aggregated_df

def group(df):
    dictionary = {}

    for idx, j in enumerate(df['id']):
        senderFederation = j.split('/')[0].strip()  # get federation
        if senderFederation in dictionary:
            dictionary[senderFederation]['count'] += df["count"][idx]
            dictionary[senderFederation]['avg'] += df["average_response_time"][idx]
            dictionary[senderFederation]['counter'] += 1
        else:
            dictionary[senderFederation]={}
            dictionary[senderFederation]['count'] = df["count"][idx]
            dictionary[senderFederation]['avg'] = df["average_response_time"][idx]
            dictionary[senderFederation]['counter'] = 1

    sender_average = {federation: [dictionary[federation]['count'] // dictionary[federation]['counter'], dictionary[federation]['avg'] // dictionary[federation]['counter']] for federation in dictionary}

    return sender_average

def plot_confusion_matrix(groupdf,gtotalisec):
    newMsgs={f'{tipo1}': valori1[1] for tipo1, valori1 in gtotalisec.items()}
    old_hist={f'{tipo1}': valori1[1] for tipo1, valori1 in groupdf.items()}

    with open(FILE_SOA) as nf: devices = json.load(nf)["devices"]

    TP=sum(elem for elem in old_hist.values())
    FN=sum(elem for elem in newMsgs.values())
    FP=len(devices)*2
    TN=0
    TOT=TP+FN+FP+TN

    confusion_m = np.matrix([[TP, FP], [FN, TN]])
    annot_text = np.matrix([["TP\n"+str(round(TP/TOT*100,2))+"%", "FP\n"+str(round(FP/TOT*100,2))+"%"], ["FN\n"+str(round(FN/TOT*100,2))+"%", "TN\n"+str(round(TN/TOT*100,2))+"%"]])

    fig = plt.figure(figsize=(8, 4))
    ax = plt.subplot(1, 2, 2)
    plt.imshow(confusion_m, interpolation='nearest', cmap=plt.cm.Blues)

    rows, cols = confusion_m.shape
    for i in range(rows):
        for j in range(cols):
            if i==0 and j==0:
                plt.text(j, i, annot_text[i, j], horizontalalignment='center', verticalalignment='center', color='white', fontsize=14)
            else:
                plt.text(j, i, annot_text[i, j], horizontalalignment='center', verticalalignment='center', color='black', fontsize=14)
    # min_val = min([TP,TN,FP,FN])
    # max_val = max([TP,TN,FP,FN])
    # sns.heatmap(confusion_m, vmin=min_val,vmax=max_val,linewidth=0.5,annot=annot_text,fmt="s",yticklabels=False,xticklabels=False,ax=axs,cmap="Blues")

    precision=round(TP/(TP+FP),2)
    recall=round(TP/(TP+FN),2)
    F1Score=round(2*(precision*recall)/(precision+recall),2)
    plt.title(f'Precision: {precision}, Recall: {recall}, F1: {F1Score}')
    plt.xticks([])
    plt.yticks([])
    plt.savefig("experiments/plot/confusion_m.png", bbox_inches='tight')
    plt.close(fig)

def plot_matrix_communities(groupdf,gtotalisec):
    
    newMsgs={f'{tipo1}': valori1[1] for tipo1, valori1 in gtotalisec.items()}
    old_hist={f'{tipo1}': valori1[1] for tipo1, valori1 in groupdf.items()}

    _, axs = plt.subplots(1, len(newMsgs.keys()))

    with open(FILE_SOA) as nf: devices = json.load(nf)["devices"]


    for idx,community in enumerate(newMsgs.keys()):
         
        k=idx
        label=community
        #tot=1
        
        TP=old_hist[community]
        FN=newMsgs[community]
        FP=len(devices)
        TN=0
        TOT=TP+FN+FP+TN
        confusion_m = np.matrix([[TP, FP], [FN, TN]])
        annot_text = np.matrix([["TP\n"+str(round(TP/TOT*100,2))+"%", "FP\n"+str(round(FP/TOT*100,2))+"%"], ["FN\n"+str(round(FN/TOT*100,2))+"%", "TN\n"+str(round(TN/TOT*100,2))+"%"]])


        # min_val = min([TP,TN,FP,FN])
        # max_val = max([TP,TN,FP,FN])
        # sns.heatmap(confusion_m, vmin=min_val,vmax=max_val, linewidth=0.5,annot=annot_text,fmt="s",yticklabels=False,xticklabels=False,ax=axs[i],cmap="Blues")
        axs[k].imshow(confusion_m, interpolation='nearest', cmap=plt.cm.Blues)
        axs[k].set_title(label)
        axs[k].set_xticks([])
        axs[k].set_yticks([])

        rows, cols = confusion_m.shape
        for i in range(rows):
            for j in range(cols):
                if i==0 and j==0:
                    axs[k].text(j, i, annot_text[i, j], horizontalalignment='center', verticalalignment='center', color='white', fontsize=12)
                else:
                    axs[k].text(j, i, annot_text[i, j], horizontalalignment='center', verticalalignment='center', color='black', fontsize=12)
    
        precision=round(TP/(TP+FP),2)
        recall=round(TP/(TP+FN),2)
        accuracy=round((TP+TN)/(TP+FN+TN+FP),2)
        F1Score=round(2*(precision*recall)/(precision+recall),2)
        print(label, precision, recall, accuracy)
    
    # min_val = min([TP,TN,FP,FN])
    # max_val = max([TP,TN,FP,FN])
    # sns.heatmap(confusion_m, vmin=min_val,vmax=max_val,linewidth=0.5,annot=annot_text,fmt="s",yticklabels=False,xticklabels=False,ax=axs,cmap="Blues")

    # precision=round(TP/(TP+FP),2)
    # recall=round(TP/(TP+FN),2)
    # F1Score=round(2*(precision*recall)/(precision+recall),2)
    # plt.title(f'Precision: {precision}, Recall: {recall}, F1: {F1Score}')
    # plt.xticks([])
    # plt.yticks([])
    plt.savefig("experiments/plot/community_matrix.png", bbox_inches='tight')

def plot_response_time(groupdf,gsec):
    old_hist={f'{tipo1}': valori1[1] for tipo1, valori1 in groupdf.items()}
    new_hist={f'{tipo1}': valori1[1]+valori2[1] for (tipo1, valori1),(_, valori2) in zip(groupdf.items(),gsec.items())}

    old_hist = {k: v / 1000 for k, v in old_hist.items()}
    new_hist = {k: v / 1000 for k, v in new_hist.items()}
    oldMissingKeys = old_hist.keys() - new_hist.keys()
    for key in oldMissingKeys:
    	new_hist[key] = 0.0
#    new_hist['c']=0.877

    categories = list(old_hist.keys())

    plt.rcParams.update({'font.size': 12})
    fig, ax = plt.subplots(layout='constrained')

    x = np.arange(len(categories))
    bar_width = 0.3

    # Disegna le barre
    bars1 = ax.bar(x, old_hist.values(), width=bar_width, label='Naive', color=COLORS_TYPE["Naive"], edgecolor='black')
    bars2 = ax.bar(x + bar_width, new_hist.values(), width=bar_width, label='SHIELD', color=COLORS_TYPE["SHIELD"], edgecolor='black')

	# Aggiungi etichette sopra le barre
    for bar in bars1:
    	ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05, 
		    f'{bar.get_height():.2f}', ha='center', va='bottom', fontsize=7)

    for bar in bars2:
    	ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05, 
		    f'{bar.get_height():.2f}', ha='center', va='bottom', fontsize=7)

# Personalizza l'aspetto del grafico
    ax.set_xlabel('Communities')
    ax.set_ylabel('End-to-end time (s)')
    ax.set_title('Comparison of End-to-End Time')
    ax.set_xticks(x + bar_width / 2)  # Centrare le etichette sull'asse x
    ax.set_xticklabels(categories)
    ax.legend()

    plt.savefig("experiments/plot/response_time.png", bbox_inches='tight')

def runGraphs():

    AG_TIME_FILE="experiments/data/ag_time.csv"
    NOSEC_FOLDER="experiments/data/resultNoSec/"
    SEC_FOLDER="experiments/data/resultSec/security/"
    SEC_RES_FOLDER="experiments/data/resultSec/results/"
    
    # ### Risk reduction analysis
    secure_network_file() #Write file
    plot_security() #Plot

    ### Overhead analysis
    data_tot= read_folder(NOSEC_FOLDER)
    groupdf=group(data_tot)
    data_sec = read_folder(SEC_FOLDER)
    gsec=group(data_sec)
    data_res_sec = read_security(SEC_RES_FOLDER)
    gtotalisec=group(data_res_sec)
    
    plot_confusion_matrix(groupdf,gtotalisec)
    plot_matrix_communities(groupdf,gtotalisec)
    plot_response_time(groupdf,gsec)

    # recomputation_time(AG_TIME_FILE) #Write file
    # plot_ag_time(AG_TIME_FILE) #Plot
