import os
import sys
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from transformers import TrOCRProcessor, VisionEncoderDecoderModel
import textRecognition as tr
from PIL import Image
import csv
from numpy import single

printed_processor = TrOCRProcessor.from_pretrained('microsoft/trocr-large-printed')
printed_model = VisionEncoderDecoderModel.from_pretrained('microsoft/trocr-large-printed')
class MonitorFolder(FileSystemEventHandler):
    FILE_SIZE=1000
    
    def on_created(self, event):
         #print(event.src_path, event.event_type)
         self.checkFolderSize(event.src_path)
   
    def on_modified(self, event):
        if(os.path.isfile(event.src_path) and 'anpr' in event.src_path):
            image = Image.open(event.src_path).convert("RGB")
            single_row = []
            image_path = event.src_path
            single_row.append(image_path)
            single_row.append(tr.ocr_printed_image(image, printed_processor, printed_model))
            print(single_row)
            c.writerow(single_row)
        self.checkFolderSize(event.src_path)
    
                  
    def checkFolderSize(self,src_path):
        if os.path.isdir(src_path):
            if os.path.getsize(src_path) >self.FILE_SIZE:
                #print("Time to backup the dir")
                pass
        else:
            if os.path.getsize(src_path) >self.FILE_SIZE:
                #print("very big file")
                pass
if __name__ == "__main__":
    #src_path = sys.argv[1]
    # open new file for writing - will erase file if it already exists -
    csvfile = open('/home/vantage/VideoStreamKafka/savedData.csv', 'w', newline='', encoding='utf-8')

    # make a new variable - c - for Python's CSV writer object -
    c = csv.writer(csvfile)

    # write a column headings row - do this only once -
    c.writerow( ['image_path' ,'Recognized_text'] )
    
    event_handler=MonitorFolder()
    observer = Observer()
    observer.schedule(event_handler, path='/home/vantage/VideoStreamKafka/runs/detect', recursive=True)
    print("Monitoring started")
    observer.start()
    try:
        while(True):
           time.sleep(1)
           
    except KeyboardInterrupt:
            csvfile.close()
            observer.stop()
            observer.join()
    csvfile.close()