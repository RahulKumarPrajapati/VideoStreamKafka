from time import sleep
from kafka import KafkaProducer
import json
import cv2
from kafka.admin import NewPartitions,KafkaAdminClient
import sys
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',max_request_size=101626282, buffer_memory=101626282)

def on_send_success(record_metadata):
    print('success',record_metadata)

def on_send_error(excep):
    print('error',excep)

video = cv2.VideoCapture(sys.argv[2])
print(sys.argv)
while True:
    success, frame = video.read()
    if not success:
        print("bad read!")
        break
    
    ret, buffer = cv2.imencode('.jpg', frame)
    producer.send(sys.argv[1], buffer.tobytes()).add_callback(on_send_success).add_errback(on_send_error)
    print(producer.partitions_for(sys.argv[1]))

video.release()
print('publish complete')