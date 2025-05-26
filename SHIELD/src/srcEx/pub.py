# python 3.10.12

import random
import time
from datetime import datetime, timedelta
from paho.mqtt import client as mqtt_client
import threading

names=["c1","c2","m1","m2","s1","s2","s2"]
topics = ["charging/c1", "charging/c2", "management/m1", "management/m2", "storage/s1", "storage/s2", "storage/topicinside"]
brokers = ['172.18.0.3', '172.18.0.3', '172.18.0.4', '172.18.0.4', '172.18.0.2', '172.18.0.2', '172.18.0.2']
port = [1884, 1884, 1885, 1885, 1883, 1883, 1883]

# Security section
securityTopic=['security/m1','security/m2','security/m1','security/m2','security/m2', 'security/m1', 'security/m1', 'security/m2', 'security/s2', 'security/m1', 'security/m2', 'security/s1']  # receiver (who's gonna unsubscribe)
secNames=["c2","c2","c1","c1","m1", "m2", "s1", "s1", "s1", "s2", "s2", "s2"]  # senders
secBrokers = ['172.18.0.3', '172.18.0.3', '172.18.0.3', '172.18.0.3','172.18.0.4', '172.18.0.4','172.18.0.2','172.18.0.2', '172.18.0.2', '172.18.0.2', '172.18.0.2', '172.18.0.2']
secPort = [1884, 1884, 1884, 1884, 1885, 1885, 1883, 1883, 1883, 1883, 1883, 1883]


class Pub:
    def __init__(self, name, topic, broker='172.18.0.4', port = 1884, msg="END", community=''):
        self.client_id=str(random.randint(0, 1000))
        self.name=name
        self.topic=topic
        self.broker=broker
        self.port=port
        self.msg=msg
        self.client=self.connect_mqtt()
        self.community=community
        
    def connect_mqtt(self):
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connect, return code %d\n", rc)

        client = mqtt_client.Client(self.client_id)
        # client.username_pw_set(username, password)
        client.on_connect = on_connect
        client.connect(self.broker, self.port)
        return client

    def publish(self, td, client):
        client.loop_start()
        start = datetime.now()
        end = start + timedelta(seconds=td)
        while datetime.now()<end:
            
            msg = f"{self.name}/ message / {datetime.now()}"
            result = client.publish(self.topic, msg)
            # result: [0, 1]
            status = result[0]
            
            if status == 0:
                #print(f"Sent `{msg}` to topic `{self.topic}`")
                print(f"Sent `{msg}`")
            else:
                print(f"Failed to send message to topic {self.topic}")
        
        self.client.loop_stop()

    def publishSecExp(self, td, client):
        client.loop_start()
        start = datetime.now()
        end = start + timedelta(seconds=td)
        while datetime.now()<end:
            
            msg = f"{self.name}/ messageEXP / {datetime.now()}"
            result = client.publish(self.topic, msg)
            # result: [0, 1]
            status = result[0]
            
            if status == 0:
                #print(f"Sent `{msg}` to topic `{self.topic}`")
                print(f"Sent `{msg}`")
            else:
                print(f"Failed to send message to topic {self.topic}")
        
        self.client.loop_stop()

    def publishSubSec(self, client, dst):
        
        self.client.loop_start()
        msg = f"{self.name}/ SUBSEC.{dst} / {datetime.now()}"
        result = client.publish(self.topic, msg)
        # result: [0, 1]
        status = result[0]
        
        if status == 0:
            #print(f"Sent `{msg}` to topic `{self.topic}`")
            print(f"Sent `{msg}`")
            
        else:
            print(f"Failed to send {msg} to topic {self.topic}")
        
        self.client.loop_stop()
    
    def publishSecMsg(self, client):
        
        self.client.loop_start()
        msg = f"{self.name}/ security / {datetime.now()}"
        result = client.publish(self.topic, msg)
        # result: [0, 1]
        status = result[0]
        
        if status == 0:
            #print(f"Sent `{msg}` to topic `{self.topic}`")
            print(f"Sent `{msg}`")
            
        else:
            print(f"Failed to send {msg} to topic {self.topic}")
        
        self.client.loop_stop()

    def publishSub(self, client, dst):
        self.client.loop_start()
            
        msg = f"{self.name}/ SUBSCRIBE.{dst} / {datetime.now()}"
        result = client.publish(self.topic, msg)
        # result: [0, 1]
        status = result[0]
        
        if status == 0:
            #print(f"Sent `{msg}` to topic `{self.topic}`")
            print(f"Sent `{msg}`")
            
        else:
            print(f"Failed to send message to topic {self.topic}")

        self.client.loop_stop()

    def publishUnsub(self, client, receiver):
        self.client.loop_start()
            
        msg = f"{self.name}/ UNSUB.{receiver} / {datetime.now()}"
        result = client.publish(self.topic, msg)
        # result: [0, 1]
        status = result[0]
        
        if status == 0:
            #print(f"Sent `{msg}` to topic `{self.topic}`")
            print(f"Sent `{msg}`")
            
        else:
            print(f"Failed to send message to topic {self.topic}")

        self.client.loop_stop()

    def publishEnd(self):
        self.client.loop_start()
            
        msg = f"{self.name}/ {self.msg} / {datetime.now()}"
        result = self.client.publish('endtopic', msg)
        # result: [0, 1]
        status = result[0]
        
        if status == 0:
            #print(f"Sent `{msg}` to topic `{self.topic}`")
            print(f"Sent `{msg}`")
            
        else:
            print(f"Failed to send message to topic {self.topic}")

        self.client.loop_stop()

def run():
    threads=[]
    
    for topic,name,broker,portsec in zip(securityTopic,secNames, secBrokers, secPort):
        pub=Pub(name, topic, broker, portsec)
        pub.publishUnsub()
        #time.sleep(0.5)
        pub.publishSub()
        #time.sleep(0.5)
    
    td=2 # experiment duration
    for idx,topic in enumerate(topics):
        
        pub=Pub(names[idx], topic, broker=brokers[idx], port=port[idx])
        threads.append(threading.Thread(target=pub.publish, args=(td,)))
    
    for thread in threads:
        thread.start()
    

    time.sleep(td+4)
    pub=Pub("end", topic, broker=brokers[idx], port=port[idx])
    pub.publishEnd()
        
if __name__ == '__main__':
    run()
