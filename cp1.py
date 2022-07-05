import time
from kafka import KafkaConsumer
from kafka import TopicPartition
import json
import numpy as np
import cv2
import warnings
import imageio
#warnings.simplefilter("ignore", DeprecationWarning)
import imageio
import os,sys
consumer = KafkaConsumer(
    sys.argv[1],
    bootstrap_servers='localhost:9092',fetch_max_bytes=101626282)
print(sys.argv)
img_lst = []
dir_no=0
start_time = time.time()
for msg in consumer:
    nparr = np.fromstring(msg.value, np.uint8)
    img_np = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img_np is None:
        print('true')
        break
    print(len(img_lst))
    f= img_np.copy()
    f = cv2.resize(f,(1200,500))
    cv2.imshow("Consumer",img_np)
    cv2.waitKey(1)
    img_np = cv2.cvtColor(img_np,cv2.COLOR_RGB2BGR)
    img_lst.append(img_np)
    if time.time() - start_time >= 30:
        print(len(img_lst))
        path = '/home/vantage/VideoStreamKafka/data'
        dir_name = path + '/'+ sys.argv[1]+ '_' + str(dir_no)
        os.mkdir(dir_name)
        filename = dir_name + '/processed.mp4'
        dir_no += 1
        imageio.mimsave(filename, img_lst, fps=30)
        img_lst=[]
        start_time = time.time()
cv2.destroyAllWindows()
#capture.release()