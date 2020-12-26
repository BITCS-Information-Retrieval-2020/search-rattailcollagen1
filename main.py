"""Create the index from the data crawled"""

from searcher.DataProcess import DataProcess
from searcher.DatabaseAccess import DatabaseAccess
from searcher.ESClient import ESClient
import argparse
import json
import logging
logging.basicConfig(level=logging.WARNING)

def load_mongodb(config_path):
    """load mongodb from a json file"""
    with open(config_path, 'r', encoding='utf-8') as config_json:
        config = json.load(config_json)
    mongodb_path = config['mongodb_path']
    DBer = DatabaseAccess()
    DBer.import_json_db(db_path = mongodb_path)

    print('load_mongodb: Done!')

def build_indices(config_path):
    """build ES indices using the current mongodb database"""
    with open(config_path, 'r', encoding='utf-8') as config_json:
        config = json.load(config_json)
    batch_size = config['batch_size']
    Dper = DataProcess(delete_indices=True, batch_size=batch_size)
    Dper.process()

    print('build_indices: Done!')

def test_query(config_path):
    """check if the elasticsearch can work independently"""
    esclient = ESClient(delete=False)
    query = {
        "type": 2,
        "top_number": 10,
        "query_text": "machine"
    }
    rest = esclient.search(query)
    logging.warning(rest)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default='test_query', type=str)
    parser.add_argument("--config", type=str, default="./config.json", help='the path to config.json')
    args = parser.parse_args()

    if args.mode == 'load_mongodb':
        load_mongodb(config_path = args.config)
    elif args.mode == 'build_indices':
        build_indices(config_path = args.config)
    elif args.mode == 'test_query':
        test_query(config_path = args.config)
    else:
        print('Invaild action!')