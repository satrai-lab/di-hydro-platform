import json
from srcEx.pub import Pub

def runPublish():

    f = open('data/v2x_network.json')

    data = json.load(f)

    edges=data['edges']

    pubDevices=set()
    subs=[]
    pubs=[]

    def findCommunity(id):
        for device in data['devices']:
            if device['id']==id:
                return device['community']

    def retrievePub(pubedge):
        for pub,client in pubs:
            if pub.name==pubedge:
                return pub,client

    for edge in edges:
        pubedge=edge[0]
        pubCommunity=findCommunity(pubedge)

        #print(f'edge: {edge}')
        if pubCommunity:
            if pubedge not in pubDevices:
                pub=Pub(pubedge,topic=f'{pubCommunity}/{pubedge}', broker='172.18.0.2', port=1883, community=pubCommunity)
                client = pub.connect_mqtt()
                pubDevices.add(pubedge)
                pubs.append([pub,client])
            else:
            
                pub,client=retrievePub(pubedge)

    return pubs
