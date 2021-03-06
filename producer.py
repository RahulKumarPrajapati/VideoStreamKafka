from time import sleep
from kafka import KafkaProducer
import json
import cv2
from kafka.admin import NewPartitions,KafkaAdminClient

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',max_request_size=101626282, buffer_memory=101626282)
""" admin_client = KafkaAdminClient()
part=NewPartitions(2)
m={'frame': part}
admin_client.create_partitions(m) """

def on_send_success(record_metadata):
    print('success',record_metadata)

def on_send_error(excep):
    print('error',excep)

video = cv2.VideoCapture(r'/home/vantage/Downloads/car_video6.mp4')

while True:
    success, frame = video.read()

    if not success:
        print("bad read!")
        break

    #ret, buffer = cv2.imencode('.jpg', frame)
    producer.send('frame', bytearray(frame)).add_callback(on_send_success).add_errback(on_send_error)
    print(producer.partitions_for('frame'))

cv2.destroyAllWindows()
video.release()
print('publish complete')