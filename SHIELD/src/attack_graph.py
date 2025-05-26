import json
import networkx as nx

def get_risk_by_vuln(vuln):
    likelihood=1
    impact=1
    
    if "cvssMetricV2" in vuln["metrics"]:
        metricV2 = vuln["metrics"]["cvssMetricV2"][0]
        likelihood=metricV2["exploitabilityScore"]
        impact=metricV2["impactScore"]
    
    if "cvssMetricV30" in vuln["metrics"] or "cvssMetricV31" in vuln["metrics"]:
        if "cvssMetricV30" in vuln["metrics"]: metricV3 = vuln["metrics"]["cvssMetricV30"][0]
        else: metricV3 = vuln["metrics"]["cvssMetricV31"][0]
        likelihood=metricV3["exploitabilityScore"]
        impact=metricV3["impactScore"]
    return likelihood,impact

def get_vulns_by_hostid(devid,devices):
    cve_list=[]
    for host in devices:
        if host["id"] == devid:
            for iface in host["network_interfaces"]:
                if "ports" in iface.keys():
                    for port in iface["ports"]:
                        for service in port["services"]:
                            cve_list.append(service["cve_list"])
                if "applications" in iface.keys():
                    for app in iface["applications"]:
                        cve_list.append(app["cve_list"])
            return list(set([item for sublist in cve_list for item in sublist]))
    return []

"""
These functions checks the pre-post condition chaining
"""
def get_req_privilege(str_priv):
    if str_priv == "NONE" or str_priv == "LOW":
        return "NONE"
    elif str_priv == "SINGLE" or str_priv == "MEDIUM":
        return "USER"
    else:
        return "ROOT"
def get_gain_privilege(isroot, isuser, req_privilege):
    if isroot == "UNCHANGED" and isuser == "UNCHANGED":
        return get_req_privilege(req_privilege)
    elif isroot == True:
        return "ROOT"
    elif isuser == True:
        return "USER"
    else:
        return "ROOT"
def retrieve_privileges(vulnID,vulnerabilities):
    for vuln in vulnerabilities:
        if vuln["id"] == vulnID:
            if "cvssMetricV2" in vuln["metrics"]:
                metricV2 = vuln["metrics"]["cvssMetricV2"][0]
                metricCvssV2 = metricV2["cvssData"]
                
                priv_required = get_req_privilege(metricCvssV2["authentication"])
                priv_gained = get_gain_privilege(metricV2["obtainAllPrivilege"],metricV2["obtainUserPrivilege"],metricCvssV2["authentication"])
                return vuln,priv_required,priv_gained
            elif "cvssMetricV30" in vuln["metrics"] or "cvssMetricV31" in vuln["metrics"]: 
                if "cvssMetricV30" in vuln["metrics"]: metricV3 = vuln["metrics"]["cvssMetricV30"][0]
                else: metricV3 = vuln["metrics"]["cvssMetricV31"][0]
                metricCvssV3 = metricV3["cvssData"]

                priv_required = get_req_privilege(metricCvssV3["privilegesRequired"])
                priv_gained = get_gain_privilege(metricCvssV3["scope"],metricCvssV3["scope"],metricCvssV3["privilegesRequired"])
                return vuln,priv_required,priv_gained
            else:
                return vuln,"NONE","NONE"

def generate_ag_model(network_file, writeonfile=False):
    with open(network_file) as nf:
        content_network = json.load(nf)
    reachability_edges = content_network["edges"]
    vulnerabilities = content_network["vulnerabilities"]
    devices = content_network["devices"]
        
    G = nx.DiGraph()
    for r_edge in reachability_edges:
        src=r_edge[0]
        dst=r_edge[1]
        dev_vulns=get_vulns_by_hostid(dst,devices)
        for v in dev_vulns:
            vuln,precondition,postcondition = retrieve_privileges(v,vulnerabilities)

            req_node = precondition+"@"+str(src)
            gain_node = postcondition+"@"+str(dst)
            vuln_id = v+'@'+str(dst)
            
            if req_node not in G.nodes(): G.add_node(req_node, type="privilege", color="green")
            if gain_node not in G.nodes(): G.add_node(gain_node, type="privilege", color="green")
            if vuln_id not in G.nodes(): G.add_node(vuln_id, type="vulnerability", color="blue")
            if (req_node, vuln_id) not in G.edges() and (vuln_id, req_node) not in G.edges(): G.add_edge(req_node, vuln_id)
            if (vuln_id, gain_node) not in G.edges() and (gain_node, vuln_id) not in G.edges(): G.add_edge(vuln_id, gain_node)

    for node_1 in G.nodes():
        if "@" not in node_1: continue
        priv1,hostid1 = node_1.split("@")
        for node_2 in G.nodes():
            if node_1 == node_2 or "@" not in node_2: continue
            priv2,hostid2 = node_2.split("@")
            if hostid1 != hostid2: continue
            
            if priv1 == "ROOT" and priv2 == "USER": G.add_edge(node_1, node_2)
            if priv1 == "USER" and priv2 == "NONE": G.add_edge(node_1, node_2)
            if priv1 == "ROOT" and priv2 == "NONE": G.add_edge(node_1, node_2)

    if writeonfile: nx.write_graphml_lxml(G, "data/agtest.graphml")
    return G

def compute_risk_analysis(vuln_ids, vulns_list):
    impact_scores=[]
    exploit_scores=[]
    for v_curr in vuln_ids:
        for v_gt in vulns_list:
            if v_gt["id"] in v_curr:
                likelihood,impact=get_risk_by_vuln(v_gt)
                exploit_scores.append(likelihood )
                impact_scores.append(impact)
    
    lambda_exploit_scores = []
    for expl_s in exploit_scores:
        lambda_exploit_scores.append(1/expl_s)
    
    lik_risk = sum(lambda_exploit_scores) if len(lambda_exploit_scores)>0 else 0
    imp_risk = (impact_scores[len(impact_scores)-1])/max(impact_scores) if len(impact_scores)>0 and max(impact_scores)>0 else 0

    if lik_risk>1: lik_risk=1
    if imp_risk>1: imp_risk=1

    return {
        "impact": imp_risk,
        "likelihood": lik_risk,
        "risk":(imp_risk)*(lik_risk),
    }

def generate_paths(network_file, G, src_ids=None, target_ids=None):
    if not target_ids: target_ids=[]
    node_types = nx.get_node_attributes(G,"type")

    with open(network_file) as nf:
        network = json.load(nf)
    vulnerabilities=network["vulnerabilities"]
    edges=network["edges"]

    for e in edges:
        if e[0] in src_ids:
            target_ids.append(e[1])
    
    sources,goals=[],[]
    for n in G.nodes:
        if "@" in n and node_types[n] != "vulnerability": 
            privilege,hostid=n.split("@")
            if hostid in src_ids: sources.append(n)
            elif hostid in target_ids: goals.append(n)
            # if not src_ids and not hostid in target_ids: sources.append(n)
            # if src_ids and hostid in src_ids: sources.append(n)
    
    list_risk_values=[]
    for s in sources:
        for t in goals:
            try: current_paths = list(nx.all_shortest_paths(G, source=s, target=t))
            except nx.NetworkXNoPath: current_paths=[]
            for single_path in current_paths:
                vulns_path=[]
                path_trace=''
                for node_p in single_path:
                    path_trace=path_trace+'#'+node_p
                    if node_types[node_p] == "vulnerability":
                        vulns_path.append(node_p)
                if len(vulns_path)<=0: continue
                risk_val = compute_risk_analysis(vulns_path, vulnerabilities)
                risk_val['path']=path_trace
                list_risk_values.append(risk_val)
            if len(list_risk_values)<=0: continue
            
    return list_risk_values

def check_communities(source,target,network_file):
    with open(network_file) as nf:
        devices = json.load(nf)["devices"]

    comSrc,comDst="",""
    for dev in devices:
        if dev["id"]==source: comSrc=dev["community"]
        if dev["id"]==target: comDst=dev["community"]
    return comSrc==comDst

def calculate_communities(path, network_file):
    with open(network_file) as nf:
        devices = json.load(nf)["devices"]
    
    all_communities=[]
    for node in path:
        if "@" not in node or "CVE" in node: continue
        dev_id=node.split("@")[1]
        for dev in devices:
            if dev["id"]==dev_id: all_communities.append(dev["community"])
    return len(set(all_communities))

def analyze_paths(attack_paths, networkfile):
    clients={}
    for path in attack_paths:
        trace_components=path["path"].split("#")
        source=trace_components[1].split("@")[1]
        target=trace_components[len(trace_components)-1].split("@")[1]
        key_path=source+"#"+target
        
        communities=calculate_communities(trace_components, networkfile)
        isSameCommunity=check_communities(source,target, networkfile)
        if key_path not in clients.keys():
            clients[key_path]={
                "count":1,
                "lengths":[len(trace_components)],
                "risks":[path["risk"]],
                "communities":[communities],
                "sameCommunity":isSameCommunity
            }
        else:
            clients[key_path]["count"]+=1
            clients[key_path]["lengths"].append(len(trace_components))
            clients[key_path]["risks"].append(path["risk"])
            clients[key_path]["communities"].append(communities)

    return clients