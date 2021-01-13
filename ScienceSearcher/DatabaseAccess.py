import pymongo
import json

from pymongo.errors import BulkWriteError


class DatabaseAccess:
    """
        DatabaseAccess
    """
    DEFAULT_SERVICE_PATH = 'mongodb://127.0.0.1:27017'
    DEFAULT_SERVICE_NAME = 'mongodb'
    DEFAULT_COLLECTION_NAME = 'papers'

    def __init__(self,
                 service_path=DEFAULT_SERVICE_PATH,
                 service_name=DEFAULT_SERVICE_NAME,
                 collection_name=DEFAULT_COLLECTION_NAME,
                 increment_beginning_pointer=-1,
                 increment_ending_pointer=None):
        """
            read section: (increment_beginning_pointer, increment_ending_pointer]
        :param service_path: 'mongodb://user:password@address:port/service_name'
        :param service_name: 'crawler'
        :param collection_name: 'papers'
        :param increment_beginning_pointer: -1 if default. maximum _id of the last increment data if specified.
        :param increment_ending_pointer: None if default, restrict the maximum _id
        """
        self.service_path = service_path
        self.service_name = service_name
        self.collection_name = collection_name

        self.client = pymongo.MongoClient(self.service_path)
        self.database = self.client[self.service_name]
        self.collection = self.database[self.collection_name]

        # 本次增量读取的起始指针（不含）
        self.increment_beginning_pointer = increment_beginning_pointer
        # 本次增量读取的终止指针（包含）
        self.increment_ending_pointer = increment_ending_pointer

        self.default_query_object = None

        # 本次批读取的终止指针
        self.current_ending_pointer = self.increment_beginning_pointer
        # 本次增量读取终止标志
        self.end_flag = False

        # self.batch_pointer = 0
        self.batch_size = 1

    def build_query_object(self):
        if self.increment_ending_pointer is not None:
            if not isinstance(self.increment_ending_pointer, int):
                raise ValueError('int expected instead of {}'.format(self.increment_ending_pointer))
            query_object = {'_id': {'$gt': self.current_ending_pointer,
                                    '$lte': self.increment_ending_pointer}}
        else:
            query_object = {'_id': {'$gt': self.current_ending_pointer}}

        self.default_query_object = query_object

    def read_batch(self, batch_size=1):
        """
            读取数据库，每次读取batch_size个数据，并以list返回，list中的每一个元素为一个dict。
        :param batch_size: int
        :return: a list of dict, list = [] or len(list) < batch_size  means no more data
        """
        self.batch_size = batch_size

        # 如果集合中的主键从零开始一直连续，那么：
        # batch_cursor = self.collection.find(
        #     {'_id': {'$gt': self.increment_beginning_pointer + self.batch_pointer,
        #              '$lt': self.increment_beginning_pointer + self.batch_pointer + self.batch_size}})

        # 如果集合中的主键中间存在中断，那么：
        # batch_cursor = self.collection.find({'_id': {'$gt': self.increment_beginning_pointer}}).skip(
        #     self.batch_pointer).limit(self.batch_size)

        # after delete batch_pointer
        self.build_query_object()
        batch_cursor = self.collection.find(self.default_query_object).limit(self.batch_size)

        batch_list, batch_length = self.build_batch_list(batch_cursor=batch_cursor)

        # if batch_length == self.batch_size:
        #     self.batch_pointer += self.batch_size
        # elif 0 < batch_length < self.batch_size:
        #     self.batch_pointer += batch_length
        #     self.end_flag = True
        # elif batch_length == 0:
        #     # self.batch_pointer += 0
        #     self.end_flag = True

        if batch_length == 0:
            self.end_flag = True
            return batch_list
        elif 0 < batch_length < self.batch_size:
            self.end_flag = True

        try:
            assert self.current_ending_pointer + batch_length == batch_list[-1]['_id']
        except AssertionError or Exception:
            print('集合中存在中断主键: ({},{}]'.format(self.current_ending_pointer, batch_list[-1]['_id']))

        self.current_ending_pointer = batch_list[-1]['_id']

        return batch_list

    @staticmethod
    def build_batch_list(batch_cursor):
        batch_list = []
        for item in batch_cursor:
            batch_list.append(item)
        return batch_list, len(batch_list)

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
