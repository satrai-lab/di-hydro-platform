import json
import pandas as pd

C_max = 3
C_between = ['Input Validation','Libraries or Frameworks', 
    'Attack Surface Reduction','Language Selection','Output Encoding',
    'Firewall','Resource Limitation']

# MITIGATION_FILE = "data/NIST/cwe_mitigation.csv"
# def getCweMitigation(cveid) :
#     df = pd.read_csv(MITIGATION_FILE, sep=';')
#     df = df[df['cve'] == cveid][["cve","phase","strategy"]]
#     return df.to_dict('records')

def calculate_trust(risk_vals, risk_min=0.2, risk_max=0.9):
    trust_matrix={}
    for keypath in risk_vals.keys():
        src_id,target_id=keypath.split("#")
        n=risk_vals[keypath]['count']
        l=(sum(risk_vals[keypath]['lengths'])/n)/max(risk_vals[keypath]['lengths'])
        r=max(risk_vals[keypath]['risks'])
        # o= 1 if risk_vals[keypath]['sameCommunity'] else 0
        c=max(risk_vals[keypath]['communities'])/C_max

        if r>risk_max: trust_matrix[keypath]=0
        elif r<risk_min: trust_matrix[keypath]=1
        else: 
            # trust_matrix[keypath]=round(l*c)
            trust_matrix[keypath]=round((l+c+(1-r))/3)
            # if l*c>0: trust_matrix[keypath]=1
    return trust_matrix

def mitigations_by_dev(file_network, dev_id):
    cve_list=[]
    with open(file_network) as nf:
        content = json.load(nf)
    devices=content["devices"]
    mitigations=content["mitigations"]

    for dev in devices:
        if dev["id"] in dev_id:
            for iface in dev["network_interfaces"]:
                for port in iface["ports"]:
                    for srv in port["services"]:
                        cve_list+=srv["cve_list"]
    mitigationsDev=[]                        
    for mitig in mitigations:
        if mitig["cve"] in cve_list: mitigationsDev.append(mitig)
    return mitigations

def mitigation_to_pubsub(trust_matrix, risk_vals, mitigation_list, pubs):
    
    attack_surface={}
    unsubscribes={}
    for keypath in trust_matrix.keys():
        src = keypath.split("#")[0]
        dst = keypath.split("#")[1]
        attack_surface[keypath]=[]
        unsubscribes[keypath]=0
        # print(dst, " SUBSCRIBE to the SECURITY TOPICS of ", src)

        for pub,client in pubs:
            if pub.name==src:
                pub.publishSubSec(client, dst)

        if trust_matrix[keypath] == 0:
            # print(dst, " UNSUBSCRIBE to the OPERATIONAL TOPICS of ", src)
            for pub,client in pubs:
                if pub.name==src:
                    pub.publishUnsub(client, dst)            

            unsubscribes[keypath]+=1
        if trust_matrix[keypath] == 1:
            # print(dst, " SUBSCRIBE to the OPERATIONAL TOPICS of ", src)

            for pub,client in pubs:
                if pub.name==src:
                    pub.publishSub(client, dst)

            send_msg=False
            mitig_list=""
            for mitigation in mitigation_list:
                if mitigation["strategy"] in C_between: 
                    send_msg=True
                    mitig_list+=mitigation["strategy"]+":val"+"#"
                    if mitigation["cve"] not in attack_surface[keypath]: attack_surface[keypath].append(mitigation["cve"])
            
            if send_msg:
                msg="sender:"+src+"@"+mitig_list
                # print(src, " PUBLISH to its SECURITY TOPICS the message ", msg)

                for pub,client in pubs:
                    if pub.name==src:
                        pub.publishSecMsg(client)            

    return attack_surface, unsubscribes
