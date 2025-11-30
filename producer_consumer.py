import socket
from confluent_kafka import Producer, Consumer

conf_prod = {'bootstrap.servers': 'localhost:9092',
          'client.id': socket.gethostname(),
          'acks': 'all',
          'compression.type': 'none',
          'retries': 5}
producer = Producer(conf_prod)

conf_cons = {'bootstrap.servers': 'localhost:9092', 
             'group.id': 'test-group',   	
             'auto.offset.reset': 'earliest',  
             'enable.auto.commit': True}  		
consumer = Consumer(conf_cons)