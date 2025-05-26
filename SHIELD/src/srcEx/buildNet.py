import json
from sub import Sub
import threading
import csv
import time

counter=0

f = open('../../data/v2x_network.json')

data = json.load(f)

def findCommunity(id):
    for device in data['devices']:
        if device['id']==id:
            return device['community']
        
def run():
    
    edges=data['edges']

    subDevices=set()
    subs=[]
   
    def retrieveSub(subedge):
        for sub,client in subs:
            if sub.id==subedge:
                return sub,client

    for edge in edges:
        pubedge=edge[0]
        subedge=edge[1]
        subCommunity=findCommunity(subedge)
        pubCommunity=findCommunity(pubedge)

        #print(f'edge: {edge}')

        if subedge not in subDevices:
            sub=Sub(subedge, broker='172.18.0.2', port=1883, community=subCommunity)
            client = sub.connect_mqtt()
            #sub.subscribe(client, '' , sec=True)
            subDevices.add(subedge)
            subs.append([sub,client])
            #print(f'{sub} named {subedge} ADDED')
        else:
            sub,client=retrieveSub(subedge)
            #print(f'retrieved {sub}')

        if pubCommunity and subCommunity:
            sub.subscribe(client,f'{pubCommunity}/{pubedge}')
            print(f'{sub.id} SUBBED to {pubCommunity}/{pubedge}')

    return subs

if __name__ == "__main__":
    subs=run()
    
    def writeOnCsv():
        global counter
        
        while True:
                
            file_path=f'../../experiments/data/resultNoSec/results{counter}.csv'
            file_pathsecurityModule=f'../../experiments/data/resultSec/security/secResults{counter}.csv'
            file_pathsec=f'../../experiments/data/resultSec/results/results{counter}.csv'

            time.sleep(4)
            
            write=False
            for sub,_ in subs:
                if len(sub.table.items()):
                    write=True

            if write:
                
                with open(file_path, mode='w', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames=['id', 'sender', 'count', 'average_response_time'])
                    writer.writeheader()
                    for sub,_ in subs:
                        #print(sub.table)

                        for k,v in sub.table.items():
                            if k != 'end':
                                times=sub.responses[k]
                                avg=int(sum(times)/len(times))
                                tmpsub=sub.community+'/'+sub.id                                                        
                                tmppub=findCommunity(k)+'/'+k
                                writer.writerow({'id':tmpsub , 'sender': tmppub, 'count': v, 'average_response_time':avg})
                        sub.table={}  # reset table

                counter+=1
            
            write=False
            for sub,_ in subs:
                if len(sub.tableSec.items()):
                    write=True
            
            if write:
                
                with open(file_pathsecurityModule, mode='w', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames=['id', 'sender', 'count', 'average_response_time'])
                    writer.writeheader()
                    for sub,_ in subs:
                        #print(sub.table)

                        for k,v in sub.tableSec.items():
                            times=sub.responsesSec[k]
                            avg=int(sum(times)/len(times))
                            tmpsub=sub.community+'/'+sub.id
                            tmppub=findCommunity(k)+'/'+k
                            writer.writerow({'id':tmpsub, 'sender': tmppub, 'count': v, 'average_response_time':avg})
                        sub.tableSec={}  # reset table

                counter+=1

            write=False
            for sub,_ in subs:
                if len(sub.tableSecExp.items()):
                    write=True

            if write:
                
                with open(file_pathsec, mode='w', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames=['id', 'sender', 'count', 'average_response_time'])
                    writer.writeheader()
                    for sub,_ in subs:
                        #print(sub.table)

                        for k,v in sub.tableSecExp.items():
                            if k != 'end':
                                times=sub.responsesSecExp[k]
                                avg=int(sum(times)/len(times))
                                tmpsub=sub.community+'/'+sub.id
                                tmppub=findCommunity(k)+'/'+k
                                writer.writerow({'id':tmpsub , 'sender': tmppub, 'count': v, 'average_response_time':avg})
                        sub.tableSecExp={}  # reset table

                counter+=1


    threads=[]
    
    for _,client in subs:
        thread=threading.Thread(target=client.loop_forever)    
        threads.append(thread)
        
    for thread in threads:      
        thread.start()
    
    wt=threading.Thread(target=writeOnCsv, daemon=True)
    wt.start()
