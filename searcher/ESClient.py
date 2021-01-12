from elasticsearch import Elasticsearch, helpers
import ipdb, random, time, json, pprint, logging, math
from time import sleep
class ESClient:

    def __init__(self, ip_port, auth=('elastic', 'elastic123'), index_name='papers', video_index_name='video', delete=False):
        self.es = Elasticsearch(ip_port, http_auth=auth)
        self.index_name, self.video_index_name = index_name, video_index_name
        self.mapping_vs = {
            'properties': {
                "timeStart": {'type': 'text'},
                "timeEnd": {'type': 'text'},
                "sentence": {'type': 'keyword'},
                'title': {'type': 'keyword'},
                "videoPath": {'type': 'text'},
                'paper_id': {'type': 'integer'},
            }
        }
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
                # _all field
                'full_field': {
                    'type': 'text',
                }
            }
        }
        # if not exist, create the index
        if delete:
            if self.es.indices.exists(index=self.index_name):
                self.es.indices.delete(index=self.index_name)
                print(f'[!] delete index: {self.index_name}')
            if self.es.indices.exists(index=self.video_index_name):
                self.es.indices.delete(index=self.video_index_name)
                print(f'[!] delete index: {self.video_index_name}')
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name)
            self.es.indices.put_mapping(body=self.mapping, index=self.index_name)
            print(f'[!] create index: {self.index_name}')
            
        if not self.es.indices.exists(index=self.video_index_name):
            self.es.indices.create(index=self.video_index_name)
            self.es.indices.put_mapping(body=self.mapping_vs, index=self.video_index_name)
            print(f'[!] create index: {self.video_index_name}')
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
        self.es.indices.put_settings(
            index=self.video_index_name,
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
            count_paper = self.es.count(index=self.index_name)['count']
            count_video = self.es.count(index=self.video_index_name)['count']
            actions = []
            for i, item in enumerate(data):
                # video index
                for s in item['videoStruct']:
                    actions.append({
                        'timeStart': s['timeStart'],
                        'timeEnd': s['timeEnd'],
                        'sentence': s['sentence'],
                        'title': item['title'],
                        'videoPath': item['videoPath'],
                        '_index': self.video_index_name,
                        '_id': count_video,
                        'paper_id': item['_id'] 
                    })
                    count_video += 1
                
                # paper index
                item['_index'] = self.index_name
                item['_id'] = item['_id']
                item.pop('videoStruct')
                actions.append(item)
            helpers.bulk(self.es, actions)
        except Exception as e:
            print(e)
            return False
        return True

    def search(self, query):
        '''
            query: query的解析在类内实现
            return: a list of dicts
        '''
        logging.info(f'QUERY: {query}')
        try:
            mode, topn = query['type'], query['top_number']
            if mode == 0:
                rest = self.search_mode_1(query['query_text'], topn)
            elif mode == 1:
                rest = self.search_mode_2(query['query_text'], query['operator'], topn)
            elif mode == 2:
                rest = self.search_mode_3(query['query_text'], topn)
            logging.info(f'REST: {rest}')
            return rest
        except Exception as e:
            logging.info(f'[!] search failed: {e}')
            return []
        
    def search_by_id(self, id_):
        dsl = {
            'query': {
                'match': {'_id': id_}
            }
        }
        hits = self.es.search(
            index=self.index_name,
            body=dsl
        )
        # rest = [h['_source'] for h in hits['hits']['hits']]
        rest = []
        for h in hits['hits']['hits']:
            h['_source']['_id'] = h['_id']
            rest.append(h['_source'])
        return rest[0]
        
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
        #rest = [h['_source'] for h in hits['hits']['hits']]
        rest = []
        for h in hits['hits']['hits']:
            h['_source']['_id'] = h['_id']
            rest.append(h['_source'])
        return rest
        
    def search_mode_2(self, query, operator, topn):
        bool_ = {'must': [], 'must_not': [], 'should': []}
        for (key, value), op in zip(query.items(), operator):
            if op == 'OR':
                value = value.strip().split()
                for v in value:
                    bool_['should'].append({"wildcard": {key: f'*{v}*'}})
            elif op == 'AND':
                bool_['must'].append({"match": {key: value}})
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
        # rest = [h['_source'] for h in hits['hits']['hits']]
        rest = []
        for h in hits['hits']['hits']:
            h['_source']['_id'] = h['_id']
            rest.append(h['_source'])
        return rest
    
    def search_mode_3(self, query_text, topn):
        value = query_text.strip().split()
        dsl = {
            'query': {
                'bool': {
                    'should': [
                        {'wildcard': {'sentence': f'*{v}*'}} for v in value
                    ]
                }
            }
        }
        print(dsl)
        hits = self.es.search(
            index=self.video_index_name,
            body=dsl,
            size=topn,
        )
        rest = [h['_source'] for h in hits['hits']['hits']]
        return rest

    def get_all_title(self, titles):
        while True:
            body = {
                "_source": ["title"],
                "query": {
                    "match_all": {}
                }
            }
            scroll = '5m'
            size = 1000
            res = self.es.search(
                index=self.index_name,
                scroll=scroll,
                size=size,
                body=body
            )
            all_data = res.get("hits").get("hits")
            scroll_id = res["_scroll_id"]
            total = res["hits"]["total"]["value"]
            for i in range(math.ceil(total / size)):
                res = self.es.scroll(scroll_id=scroll_id, scroll='5m')
                all_data += res["hits"]["hits"]

            for item in all_data:
                cur_title = item['_source']['title']
                if cur_title not in titles:
                    titles.append(cur_title)
            sleep(43200)


if __name__ == "__main__":
    # test data
    with open('papers.json', 'rb') as f:
        test_data = json.load(f)
    esclient = ESClient('10.1.114.121:9200', delete=True)
    
    # create index
    if esclient.update_index(test_data, len(test_data)):
        print('write into ES successfully')
    else:
        print('write into ES failed')
    time.sleep(3)
    # search
    query1 = {
        "type": 0,
        "top_number": 10,
        "query_text": "CNN"
    }
    # rest = esclient.search(query1)
    # pprint.pprint(f'rest1: {rest}')

    query2 = {
        "type": 1,
        "top_number": 10,
        "query_text": {
            "title": "Oral NeurIPS Spotlight",
            "authors": "",
            "abstract": "",
            "content": "",
            "year": "",
        },
        "operator": ["OR", "", "", "", ""]
    }
    # rest = esclient.search(query2)
    # pprint.pprint(f'rest2[{len(rest)}]: {rest}')
    # print(len(rest))
    # for item in rest:
    #     ipdb.set_trace()
    
    # rest = esclient.search_by_id(0)
    # print(rest)
    # exit()

    query3 = {
        "type": 2,
        "top_number": 10,
        "query_text": "github experiment"
    }
    rest = esclient.search(query3)
    pprint.pprint(f'{len(rest)}')
    for item in rest:
        paper = esclient.search_by_id(item['paper_id'])
        ipdb.set_trace()
