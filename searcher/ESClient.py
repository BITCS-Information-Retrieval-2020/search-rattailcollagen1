class ESClient:

    def __init__(self):
        pass
    
    def update_index(self, data, batch_size):
        '''
            建立索引，返回是否成功。
            data：传过来的一个list of dict
            batch_size：本次传过来的dict的数量，
                dict中每一个元素是与该论文有关的所有信息

            return: bool, 返回是否成功

        '''

    def search(self, query):
        '''
            query: query的解析在类内实现
            return: a list of dicts
        '''