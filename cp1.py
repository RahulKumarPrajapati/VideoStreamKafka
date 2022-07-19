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
from constant import *

consumer = KafkaConsumer(
    sys.argv[1],
    bootstrap_servers=BOOTSTRAP_SERVER,
    fetch_max_bytes=FETCH_MAX_BYTES,
    auto_offset_reset='earliest',
    group_id=sys.argv[1]+"_group_1"
    )

img_lst = []
dir_no=0
start_time = time.time()
i =0 
for msg in consumer:
    nparr = np.frombuffer(msg.value, np.uint8)
    img_np = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img_np is None:
        print('true')
        break
    print(i)
    img_np = cv2.cvtColor(img_np,cv2.COLOR_RGB2BGR)
    img_lst.append(img_np)
    if time.time() - start_time >= 30:
        print(len(img_lst))
        path = STREAM_VIDEO_STORAGE_PATH
        dir_name = path + '/'+ sys.argv[1]+ '_' + str(dir_no)
        os.mkdir(dir_name)
        filename = dir_name + '/processed.mp4'
        dir_no += 1
        imageio.mimsave(filename, img_lst, fps=30)
        img_lst=[]
        start_time = time.time()
    i +=1
cv2.destroyAllWindows()
#capture.release()