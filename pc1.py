from time import sleep
from kafka import KafkaProducer
import json
import cv2
from kafka.admin import NewPartitions,KafkaAdminClient
import sys
from constant import *

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVER,max_request_size=MAX_REQUEST_SIZE, buffer_memory=BUFFER_MEMORY)

def on_send_success(record_metadata):
    #print('success',record_metadata)
    pass

def on_send_error(excep):
    print('error',excep)

video = cv2.VideoCapture(sys.argv[2])
i =0 
total_partitions = producer.partitions_for(sys.argv[1])
if len(total_partitions) == 1:
    admin_client = KafkaAdminClient()
    part=NewPartitions(2)
    m={sys.argv[1]: part}
    admin_client.create_partitions(m)
while True:
    success, frame = video.read()
    if not success:
        print("bad read!")
        break
    
    ret, buffer = cv2.imencode('.jpeg', frame)
    producer.send(sys.argv[1], buffer.tobytes()).add_callback(on_send_success).add_errback(on_send_error)
    print(i)
    #break
    i+=1
video.release()
print('publish complete')