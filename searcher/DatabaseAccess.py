import pymongo


class DatabaseAccess:
    """
        一点设想：希望部署MongoDB的同学可以将爬虫模块提供的MongoDB打包（拷贝）至
        我们的/data文件夹下某处，然后通过运行一些指令（可以用Python单独写一个简单的脚本）的方式
        来将这个MongoDB部署到本机。之后再利用该类（DatabaseAccess）来对MongoDB中的数据进行读取。
        外层调用程序将通过self.read函数来批量读取MongoDB中的dict数据。
    """

    SERVICE_PATH = 'mongodb://127.0.0.1:27017'
    SERVICE_NAME = 'mongodb'
    COLLECTION_NAME = 'papers'

    def __init__(self):
        self.client = pymongo.MongoClient(DatabaseAccess.SERVICE_PATH)
        self.database = self.client[DatabaseAccess.SERVICE_NAME]
        self.collection = self.database[DatabaseAccess.COLLECTION_NAME]

        self.batch_pointer = 0
        self.batch_size = 1

    def read(self, number=1):
        """
            读取数据库，每次读取number个数据，
            并以list返回，list中的每一个元素为
            一个dict
            Parameters:
                number: int
            Return:
                a list of dict
        """
        return self.read_batch(batch_size=number)

    def read_batch(self, batch_size=1):
        """
            读取数据库，每次读取number个数据，
            并以list返回，list中的每一个元素为
            一个dict
            Parameters:
                batch_size: int
            Return:
                a list of dict, list = [] or len(list) < batch_size  means no more data
        """
        self.batch_size = batch_size
        batch_cursor = self.collection.find(
            {'_id': {'$gte': self.batch_pointer, '$lt': self.batch_pointer + self.batch_size}})
        self.batch_pointer += self.batch_size
        return self.build_batch_list(batch_cursor=batch_cursor)

    @staticmethod
    def build_batch_list(batch_cursor):
        batch_list = []
        for item in batch_cursor:
            batch_list.append(item)
        return batch_list

    def insert(self, item):
        insert_id = self.collection.insert_one(item)
        return insert_id
