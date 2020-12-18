from .DatabaseAccess import DatabaseAccess
from .PDFProcessor import PDFProcessor
from .VideoProcessor import VideoProcessor
from .ESClient import ESClient

class DataProcess:
    
    def __init__(self, batch_size=200):
        '''Initialize an object of DataProcess

            Parameters:
                batch_size: int
                    Specify the number of items to be fetch from the MongoDB (via DatabaseAccess)
        '''

        print("DataProcess~")
        self.batchSize = batch_size
        self.DBer = DatabaseAccess()
        self.PDFer = PDFProcessor()
        self.Videoer = VideoProcessor()
        self.ESer = ESClient()

    def process(self):
        '''Fetch (self.batchSize) items and insert them into the ESClient

            Basic process:
                1. Fetch self.batchSize items via self.DBer
                2. Convert pdf and video data via self.PDFer and self.Videoer
                3. Aggregate them into a json
                4. Send the json list into the ESClient

        '''

        dataFromDB = self.DBer(self.batchSize)
        dataToESClent = []

        for item in dataFromDB:
            itemToESClient = item.copy()
            pdfPath = item['pdfPath']
            videoPath = item['videoPath']

            pdfText = self.PDFer.convert(pdfPath)
            videoStruct = self.Videoer.convert(videoPath)

            itemToESClient['pdfText'] = pdfText
            itemToESClient['videoStruct'] = videoStruct

            dataToESClent.append(itemToESClient)

        self.ESer.update_index(dataToESClent, self.batchSize)
        



        