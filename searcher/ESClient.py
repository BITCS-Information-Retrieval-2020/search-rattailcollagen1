from elasticsearch import Elasticsearch, helpers
import ipdb, random, time, json, pprint

class ESClient:

    def __init__(self, auth=('elastic', 'elastic123'), index_name='papers', delete=False):
        self.es = Elasticsearch(http_auth=auth)
        self.index_name = index_name
        self.mapping = {
            'properties': {
                'title': {
                    'type': 'keyword',
                    'copy_to': 'full_field',
                },
                'authors': {
                    'type': 'text',
                    'copy_to': 'full_field',
                },
                'abstract': {
                    'type': 'text',
                    'copy_to': 'full_field',
                },
                'publicationOrg': {
                    'type': 'text',
                    'copy_to': 'full_field',
                },
                'year': {'type': 'integer'},
                'pdfUrl': {'type': 'text'},
                "pdfPath": {'type': 'text'},
                "publicationUrl": {'type': 'text'},
                "codeUrl": {'type': 'text'},
                "datasetUrl": {'type': 'text'},
                "videoUrl": {'type': 'text'},
                "videoPath": {'type': 'text'},
                "pdfText": {
                    'type': 'text',
                    'copy_to': 'full_field',
                },
                # videoStruct array type
                "videoStruct": {
                    "properties": {
                        "timeStart": {'type': 'text'},
                        "timeEnd": {'type': 'text'},
                        "sentence": {
                            'type': 'text',
                            'copy_to': 'full_field',
                        },
                    }
                },
                # _all field
                'full_field': {
                    'type': 'text',
                },
                'hot'
            }
        }
        # if not exist, create the index
        if delete:
            self.es.indices.delete(index=self.index_name)
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name)
            self.es.indices.put_mapping(body=self.mapping, index=self.index_name)
            print(f'[!] create index: {self.index_name}')
        # set the max windows size
        self.es.indices.put_settings(
            index=self.index_name,
            body={
                'index': {
                    'max_result_window': 500000,
                    'refresh_interval': '1s',
                },
            }
        )
        # print(f'[!] init ESClient successfully')
    
    def update_index(self, data, batch_size):
        '''
            建立索引，返回是否成功。
            data：传过来的一个list of dict
            batch_size：本次传过来的dict的数量，
                dict中每一个元素是与该论文有关的所有信息

            return: bool, 返回是否成功

        '''
        try:
            count = self.es.count(index=self.index_name)['count']
            actions = []
            for i, item in enumerate(data):
                item['_index'] = self.index_name
                item['_id'] = count + i
                actions.append(item)
            helpers.bulk(self.es, actions)
        except:
            return False
        return True

    def search(self, query):
        '''
            query: query的解析在类内实现
            return: a list of dicts
        '''
        try:
            mode, topn = query['type'], query['top_number']
            if mode == 0:
                rest = self.search_mode_1(query['query_text'], topn)
            elif mode == 1:
                rest = self.search_mode_2(query['query_text'], query['operator'], topn)
            elif mode == 2:
                rest = self.search_mode_3(query['query_text'], topn)
            return rest
        except Exception as e:
            print(f'[!] search failed: {e}')
            return []
        
    def search_mode_1(self, query_text, topn):
        dsl = {
            'query': {
                'match': {'full_field': query_text}
            }
        }
        print(dsl)
        hits = self.es.search(
            index=self.index_name,
            body=dsl,
            size=topn
        )
        rest = [h['_source'] for h in hits['hits']['hits']]
        return rest
        
    def search_mode_2(self, query, operator, topn):
        bool_ = {'must': [], 'must_not': [], 'should': []}
        for (key, value), op in zip(query.items(), operator):
            if op in ['OR', 'AND']:
                bool_['should'].append({"match": {key: value}})
            elif op == 'NOT':
                bool_['must_not'].append({"match": {key: value}})
            elif not op:
                pass
            else:
                raise Exception(f'[!] unknow operator: {op}')
        dsl = {
            'query': {'bool': bool_}
        }
        print(dsl)
        hits = self.es.search(
            index=self.index_name,
            body=dsl,
            size=topn,
        )
        rest = [h['_source'] for h in hits['hits']['hits']]
        return rest
    
    def search_mode_3(self, query_text, topn):
        # https://www.elastic.co/guide/cn/elasticsearch/guide/current/nested-objects.html
        # elasticsearch flat the nested object
        dsl = {
            'query': {
                'match': {
                    'sentence': query_text
                }
            }
        }
        print(dsl)
        hits = self.es.search(
            index=self.index_name,
            body=dsl,
            size=topn,
        )
        rest = [h['_source'] for h in hits['hits']['hits']]
        return rest
        
if __name__ == "__main__":
    # test data
    with open('/home/lt/data/papers.json', 'rb') as f:
        test_data = json.load(f)
    
    esclient = ESClient(delete=True)
    # create index
    esclient.update_index(test_data, len(test_data))
    time.sleep(2)
    # search
    query1 = {
        "type": 0,
        "top_number": 5,
        "query_text": "DVERGE: Diversifying Vulnerabilities for Enhanced Robust Generation of Ensembles"
    }
    rest = esclient.search(query1)
    pprint.pprint(f'rest1: {rest}')

    query2 = {
        "type": 1,
        "top_number": 10,
        "query_text": {
            "title": "[Oral at NeurIPS 2020] DVERGE: Diversifying Vulnerabilities for Enhanced Robust Generation of Ensembles",
            "authors": "",
            "abstract": "",
            "content": "",
            "year": 2020,
        },
        "operator": ["AND", "", "", "", "NOT"]
    }
    rest = esclient.search(query2)
    pprint.pprint(f'rest2: {rest}')

    query3 = {
        "type": 2,
        "top_number": 10,
        "query_text": "machine"
    }
    rest = esclient.search(query3)
    pprint.pprint(f'rest3: {rest}')