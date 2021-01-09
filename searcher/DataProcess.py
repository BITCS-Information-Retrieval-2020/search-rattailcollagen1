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
    
    def __init__(self, config, var_file, mongodb_ip, mongodb_port, es_ip, es_port,
        connected=False, delete_indices = False, batch_size=200, local_mongo_drop_flag=True
        ):
        '''Initialize an object of DataProcess

            Parameters:
                batch_size: int
                    Specify the number of items to be fetch from the MongoDB (via DatabaseAccess)
        '''

        print("DataProcess~")
        self.currentPath = '/'.join(os.path.split(os.path.realpath(__file__))[0].split('\\'))
        self.batchSize = batch_size
        self.sleep_time = 2
        if connected:
            """connect mongodb server from remote"""
            mongodb_service_path = 'mongodb://{0}:{1}'.format(mongodb_ip, mongodb_port)
            mongodb_service_name = config['mongodb_service_name']
            mongodb_collection_name = config['mongodb_collection_name']
            mongodb_increment_beginning_pointer = var_file['mongodb_increment_beginning_pointer']
            self.DBer = DatabaseAccess(service_path=mongodb_service_path,
                        service_name=mongodb_service_name,
                        collection_name=mongodb_collection_name,
                        increment_beginning_pointer=mongodb_increment_beginning_pointer)
            print('connect_remote_mongodb: Done!')
        else:
            """load mongodb from a json file"""
            mongodb_path = config['mongodb_path']
            mongodb_increment_beginning_pointer = var_file['mongodb_increment_beginning_pointer']
            self.DBer = DatabaseAccess(increment_beginning_pointer=mongodb_increment_beginning_pointer)
            self.DBer.import_json_db(db_path=mongodb_path, drop_flag=local_mongo_drop_flag)
            print('load_mongodb: Done!')

        self.PDFer = PDFProcessor()
        self.Videoer = VideoProcessor()
        self.ESer = ESClient(ip_port='{0}:{1}'.format(es_ip, es_port), delete = delete_indices)
        print('Current path: ', self.currentPath)

    def process(self, pdf_ip, pdf_port, cache_dir_index=1):
        '''Fetch (self.batchSize) items and insert them into the ESClient

            Basic process:
                1. Fetch self.batchSize items via self.DBer
                2. Convert pdf and video data via self.PDFer and self.Videoer
                3. Aggregate them into a json
                4. Send the json list into the ESClient

            Return:
                the end index of mongodb in this reading process
        '''
        mongodb_increment_next_pointer = -1
        while True:
            """Iteratively fetch data from MongoDB"""
            dataFromDB = self.DBer.read_batch(batch_size = self.batchSize)
            if dataFromDB == []:
                break

            dataToESClient = []
            for item in dataFromDB:
                """Remove the dot symbol in the path string"""
                mongodb_increment_next_pointer = max(mongodb_increment_next_pointer, item['_id'])
                # print('_id: ', str(item['_id']))
                if item['pdfPath'] != "" and item['pdfPath'][0] == '.':
                    del item['pdfPath'][0]
                if item['pdfPath'][0:6] == "/data/":
                    item['pdfPath'] = item['pdfPath'][6:]
                if item['videoPath'] != "" and item['videoPath'][0] == '.':
                    del item['videoPath'][0]
                if item['videoPath'][0:6] == "/data/":
                    item['videoPath'] = item['videoPath'][6:]

                if item['pdfPath'] != "":
                    item['pdfPath'] = '/'.join(os.path.join('/data/cache', str(cache_dir_index), item['pdfPath']).split('\\'))
                if item['videoPath'] != "":
                    item['videoPath'] = '/'.join(os.path.join('/data/cache', str(cache_dir_index), item['videoPath']).split('\\'))
                itemToESClient = item.copy()
                
                pdfPath = item['pdfPath']
                pdf_path = '/'.join(os.path.join(self.currentPath, pdfPath[1:]).split('\\'))
                videoPath = item['videoPath']
                video_path = '/'.join(os.path.join(self.currentPath, videoPath[1:]).split('\\'))
                
                """Convert the corresponding pdf file and video file to the specific forms"""
                if pdfPath != "":
                    pdfText = self.PDFer.convert(server=pdf_ip, port=pdf_port, \
                        pdf_path=pdf_path)
                    itemToESClient['pdfText'] = pdfText
                else:
                    itemToESClient['pdfText'] = ""

                if videoPath != "":
                    videoStruct = self.Videoer.convert(video_path=video_path)
                    itemToESClient['videoStruct'] = videoStruct
                else:
                    itemToESClient['videoStruct'] = []

                dataToESClient.append(itemToESClient)
            
            """Insert dict list into the ES system"""
            # logging.warning(dataToESClient)
            status = self.ESer.update_index(data = dataToESClient, batch_size = len(dataToESClient))
            logging.info(status)
            sleep(self.sleep_time)
            """Check if the cursor is in the end of the MongoDB"""
            if len(dataToESClient) < self.batchSize:
                break
        
        return mongodb_increment_next_pointer
        
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
