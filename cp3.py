import time
from kafka import KafkaConsumer
from kafka import TopicPartition
import numpy as np
import cv2
import os,sys
from constant import *

consumer = KafkaConsumer(
    sys.argv[1],
    bootstrap_servers=BOOTSTRAP_SERVER,
    fetch_max_bytes=FETCH_MAX_BYTES,
    auto_offset_reset='earliest',
    group_id = 'g3'
    )
for msg in consumer:
    nparr = np.frombuffer(msg.value, np.uint8)
    img_np = cv2.imdecode(nparr, cv2.IMREAD_COLOR) # or file, Path, PIL, OpenCV, numpy, list

    if img_np is None:
        break
    f= img_np.copy()
    f = cv2.resize(f,(1200,500))
    cv2.imshow("Consumer",f)
    cv2.waitKey(1)
    
cv2.destroyAllWindows()