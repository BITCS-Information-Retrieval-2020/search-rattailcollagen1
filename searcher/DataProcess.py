from .DatabaseAccess import DatabaseAccess
from .PDFProcessor import PDFProcessor
from .VideoProcessor import VideoProcessor
from .ESClient import ESClient
import os
import argparse
import logging
from time import sleep
# logging.basicConfig(level=logging.WARNING)

class DataProcess:
    
    def __init__(self, delete_indices = False, batch_size=200):
        '''Initialize an object of DataProcess

            Parameters:
                batch_size: int
                    Specify the number of items to be fetch from the MongoDB (via DatabaseAccess)
        '''

        print("DataProcess~")
        self.currentPath = '/'.join(os.path.split(os.path.realpath(__file__))[0].split('\\'))
        self.batchSize = batch_size
        self.sleep_time = 2
        self.DBer = DatabaseAccess()
        self.PDFer = PDFProcessor()
        self.Videoer = VideoProcessor()
        self.ESer = ESClient(delete = delete_indices)
        print('Current path: ', self.currentPath)

    def process(self, pdf_ip, pdf_port):
        '''Fetch (self.batchSize) items and insert them into the ESClient

            Basic process:
                1. Fetch self.batchSize items via self.DBer
                2. Convert pdf and video data via self.PDFer and self.Videoer
                3. Aggregate them into a json
                4. Send the json list into the ESClient

        '''
        while True:
            """Iteratively fetch data from MongoDB"""
            dataFromDB = self.DBer.read_batch(batch_size = self.batchSize)
            if dataFromDB == []:
                break

            dataToESClient = []
            for item in dataFromDB:
                """Remove the dot symbol in the path string"""
                if item['pdfPath'] != "" and item['pdfPath'][0] == '.':
                    del item['pdfPath'][0]
                if item['videoPath'] != "" and item['videoPath'][0] == '.':
                    del item['videoPath'][0]
                itemToESClient = item.copy()
                    
                pdfPath = item['pdfPath']
                videoPath = item['videoPath']

                """Convert the corresponding pdf file and video file to the specific forms"""
                if pdfPath != "":
                    pdfText = self.PDFer.convert(server=pdf_ip, port=pdf_port, \
                    pdf_path=self.currentPath + pdfPath)
                    itemToESClient['pdfText'] = pdfText
                else:
                    itemToESClient['pdfText'] = ""

                if videoPath != "":
                    videoStruct = []#self.Videoer.convert(self.currentPath + videoPath)
                    itemToESClient['videoStruct'] = videoStruct
                else:
                    itemToESClient['videoStruct'] = []

                dataToESClient.append(itemToESClient)
            
            """Insert dict list into the ES system"""
            # logging.warning(dataToESClient)
            self.ESer.update_index(data = dataToESClient, batch_size = len(dataToESClient))
            sleep(self.sleep_time)
            """Check if the cursor is in the end of the MongoDB"""
            if len(dataToESClient) < self.batchSize:
                break
        
if __name__ == '__main__':
    """This script is to read data from MongoDB, process PDFs as well as videos, and insert
        them into the ES system. 
        We implement the process in several batches.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_size", type = int, default=50)
    args = parser.parse_args()
    
    dp = DataProcess(batch_size = args.batch_size)
    dp.process()
