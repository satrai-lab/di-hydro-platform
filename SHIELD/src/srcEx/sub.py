# python 3.10.12

import time
from paho.mqtt import client as mqtt_client
import threading
from datetime import datetime
import keyboard
import csv
import json

counter=0

cNames=["c1","c2"]
mNames=["m1","m2"]
sNames=["s1","s2"]
chargingTopics = ["management/m1", "management/m2"]
managementTopics = ["charging/c1", "charging/c2","storage/s1", "storage/s2"]
storageTopics = ["storage/topicinside"]

#securityTopics=["security/c1","security/c2","security/m1","security/m2","security/s1","security/s2"]

chargingBroker = '172.18.0.3' # broker IP and port
chargingPort = 1884
managementBroker = '172.18.0.4' # broker IP and port
managementPort = 1885
storageBroker = '172.18.0.2' # broker IP and port
storagePort = 1883

end=False

def findCommunity(id):
    f = open('../../data/v2x_network.json')

    data = json.load(f)
    for device in data['devices']:
        if device['id']==id:
            return device['community']

class Sub:
    def __init__(self, name, broker='172.18.0.3', port = 1885, community=''):
        self.table={}
        self.tableSec={}
        self.tableSecExp={}
        self.responses={}
        self.responsesSec={}
        self.responsesSecExp={}
        self.id = name
        self.broker=broker
        self.port=port
        self.community=community

    def connect_mqtt(self) -> mqtt_client:
        def on_connect(client, userdata, flags, rc):
            return
            '''
            if rc == 0:
                print(client)
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connect, return code %d\n", rc)
            '''
        client = mqtt_client.Client(self.id, clean_session=False)
        # client.username_pw_set(username, password)
        client.on_connect = on_connect
        client.connect(self.broker, self.port)
        return client

    def subscribe(self, client: mqtt_client, topic, sec=False):

        def updateSecExpTables(sender,difference):
            if sender != self.id:
                if sender in self.tableSecExp:
                    self.tableSecExp[sender] += 1
                else:
                    self.tableSecExp[sender] = 1
                    self.responsesSecExp[sender]=[]
                self.responsesSecExp[sender].append(difference)

        def update_tables(sender, difference):
            if sender != self.id:
                if sender in self.table:
                    self.table[sender] += 1
                else:
                    self.table[sender] = 1
                    self.responses[sender]=[]
                self.responses[sender].append(difference)
        
        def updateSecTable(sender, difference):
            if sender != self.id:
                if sender in self.tableSec:
                    self.tableSec[sender] += 1
                else:
                    self.tableSec[sender] = 1
                    self.responsesSec[sender]=[]
                self.responsesSec[sender].append(difference)
            
        def on_message(client, userdata, msg):
       
            received_time=datetime.now()
            decoded=msg.payload.decode()
            sender,tmp,invio=decoded.split('/')

            '''
            if sender=='end':
                
                for _ in range(10):
                    keyboard.press_and_release('q')

                print('END RECEIVED')
                
            elif sender=='endsec':
                
                for _ in range(10):
                    keyboard.press_and_release('w') 
                
                print('ENDSEC RECEIVED')

            elif sender=='endsecExp':
               
                for _ in range(10):
                    keyboard.press_and_release('e')
                
                print('ENDSECEXP RECEIVED')
            '''
            
            sent_time=datetime.strptime(invio.strip(), '%Y-%m-%d %H:%M:%S.%f')
            difference=int((received_time-sent_time).total_seconds()*1000)
            
            #print(f"difference: {difference}ms, sender:{sender}, topic: {msg.topic}")
            
            print(f'{self.id} received {decoded}')
                        
            if 'UNSUB' in tmp:
                receiver=tmp.split('.')[-1]
                #print(f'controllo tra {receiver} e {self.id}')
                if receiver.strip()==self.id.strip():
                    #print('CONTROLLO SUPERATO')
                    senderCommunity=findCommunity(sender)
                    client.unsubscribe(f'{senderCommunity}/{sender}')
                    #print(f'{self.id} UNSUBBED from {senderCommunity}/{sender}')

                    updateSecTable(sender,difference)

            elif 'SUBSCRIBE' in tmp:
                receiver=tmp.split('.')[-1]
                #print(f'controllo tra {receiver} e {self.id}')
                if receiver.strip()==self.id.strip():
                    #print('CONTROLLO SUPERATO')
                    senderCommunity=findCommunity(sender)
                    client.subscribe(f'{senderCommunity}/{sender}')
                    
                    #print(f'{self.id} SUBBED to {senderCommunity}/{sender}')

                    updateSecTable(sender,difference)
            
            elif 'SUBSEC' in tmp:
                receiver=tmp.split('.')[-1]
                #print(f'controllo tra {receiver} e {self.id}')
                if receiver.strip()==self.id.strip():
                    #print('CONTROLLO SUPERATO')
                    senderCommunity=findCommunity(sender)
                    client.subscribe(f'security/{sender}')
                    
                    #print(f'{self.id} SUBBED to {senderCommunity}/{sender}')

                    updateSecTable(sender,difference)
            elif 'security' in tmp:
                
                
                senderCommunity=findCommunity(sender)
                updateSecTable(sender,difference)
            
            elif tmp.strip()=='message':
                update_tables(sender, difference)
            else:
                updateSecExpTables(sender, difference)
                
        if sec:
            #print(f'{self.id} subbed to security/{self.id}')
            client.subscribe(f'security/{self.id}')
        else:
            #print(f'{self.id} subbed to {topic}')
            client.subscribe(topic)
        client.on_message = on_message

'''
def run():

    threads=[]
    subs = []
    for name in cNames:
        sub=Sub(name, broker=chargingBroker, port=chargingPort)
        subs.append(sub)
        client = sub.connect_mqtt()
        for topic in chargingTopics:
            sub.subscribe(client, topic)
        sub.subscribe(client, topic, sec=True)
        thread=threading.Thread(target=client.loop_forever)
        
        threads.append(thread)

    for name in mNames:
        sub=Sub(name, broker=managementBroker, port=managementPort)
        subs.append(sub)
        client = sub.connect_mqtt()
        for topic in managementTopics:
            sub.subscribe(client, topic)
        sub.subscribe(client, topic, sec=True)
        thread=threading.Thread(target=client.loop_forever)
        
        threads.append(thread)
    
    for name in sNames:
        sub=Sub(name, broker=storageBroker, port=storagePort)
        subs.append(sub)
        client = sub.connect_mqtt()
        for topic in storageTopics:
            sub.subscribe(client, topic)
        sub.subscribe(client, topic, sec=True)
        thread=threading.Thread(target=client.loop_forever)
        
        threads.append(thread)

    for thread in threads:
        
        thread.start() 

if __name__ == '__main__':
    run() # remember to press 'q' from keyboard to print results into csv file

'''