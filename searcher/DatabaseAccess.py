import pymongo
import json

from pymongo.errors import BulkWriteError


class DatabaseAccess:
    """
        一点设想：希望部署MongoDB的同学可以将爬虫模块提供的MongoDB打包（拷贝）至
        我们的/data文件夹下某处，然后通过运行一些指令（可以用Python单独写一个简单的脚本）的方式
        来将这个MongoDB部署到本机。之后再利用该类（DatabaseAccess）来对MongoDB中的数据进行读取。
        外层调用程序将通过self.read函数来批量读取MongoDB中的dict数据。
    """

    DEFAULT_SERVICE_PATH = 'mongodb://127.0.0.1:27017'
    DEFAULT_SERVICE_NAME = 'mongodb'
    DEFAULT_COLLECTION_NAME = 'papers'

    def __init__(self,
                 service_path=DEFAULT_SERVICE_PATH,
                 service_name=DEFAULT_SERVICE_NAME,
                 collection_name=DEFAULT_COLLECTION_NAME,
                 increment_beginning_pointer=-1):
        """

        :param service_path: 'mongodb://user:password@address:port/service_name'
        :param service_name: 'crawler'
        :param collection_name: 'papers'
        :param increment_beginning_pointer: -1 if default. maximum _id of the last increment data if specified.
        """
        self.service_path = service_path
        self.service_name = service_name
        self.collection_name = collection_name

        self.client = pymongo.MongoClient(self.service_path)
        self.database = self.client[self.service_name]
        self.collection = self.database[self.collection_name]

        # 上次增量数据集合的最大id
        self.increment_beginning_pointer = increment_beginning_pointer
        # TODO 最后可能修改初始值为increment_beginning_pointer，并调整read_batch
        self.batch_pointer = 0
        self.batch_size = 1

    def read_batch(self, batch_size=1):
        """
            读取数据库，每次读取batch_size个数据，
            并以list返回，list中的每一个元素为
            一个dict。
            Parameters:
                batch_size: int
            Return:
                a list of dict, list = [] or len(list) < batch_size  means no more data
        """
        self.batch_size = batch_size

        # 如果集合中的主键从零开始一直连续，那么：
        # batch_cursor = self.collection.find(
        #     {'_id': {'$gt': self.increment_beginning_pointer + self.batch_pointer,
        #              '$lt': self.increment_beginning_pointer + self.batch_pointer + self.batch_size}})

        # 如果集合中的主键中间存在中断，那么：
        batch_cursor = self.collection.find({'_id': {'$gt': self.increment_beginning_pointer}}).skip(
            self.batch_pointer).limit(self.batch_size)

        self.batch_pointer += self.batch_size
        return self.build_batch_list(batch_cursor=batch_cursor)

    @staticmethod
    def build_batch_list(batch_cursor):
        batch_list = []
        for item in batch_cursor:
            batch_list.append(item)
        return batch_list

    def import_json_db(self, db_path='./data/papers.json', drop_flag=False):
        """
            导入json格式的mongodb数据库
            # TODO 由于内存限制，可能有导入上限，如果文件过大，改用*mongoimport*工具

            examples:
                dba.import_json_db(db_path='./searcher/data/papers.json')
            mongo import:
                mongoimport --port 27030 -u sa -p Expressin@0618 -d mapdb -c bike_bak  --type=json --file bike.csv
        :param db_path: json文件路径
        :param drop_flag: drop the collection if flag is true, keep the collection otherwise.
        :return:
        """
        if drop_flag:
            self.safe_drop()
        try:
            with open(file=db_path, mode='r', encoding='utf-8')as fp:
                paper_list = json.load(fp=fp)
                print('import {} papers.'.format(len(paper_list)))
                self.collection.insert_many(paper_list)
        except FileNotFoundError as fe:
            print(fe.strerror)
        except UnicodeDecodeError as de:
            print(de.reason)
        except BulkWriteError as be:
            print('primary key conflict occurred 0and partial insertion was successful:{}'.format(be.details))

    def safe_drop(self):
        """
            为确保不会突然把远程数据库清空。
        :return:
        """
        if self.service_path == DatabaseAccess.DEFAULT_SERVICE_PATH:
            self.collection.drop()
            print('drop successfully.')
        else:
            print('Cannot drop any collection of remote database!:{}'.format(self.service_path))
