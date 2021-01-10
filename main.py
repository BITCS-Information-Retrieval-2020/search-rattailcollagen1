"""Create the index from the data crawled"""

from searcher.DataProcess import DataProcess
# from searcher.DatabaseAccess import DatabaseAccess
from searcher.ESClient import ESClient
from searcher.PDFProcessor import PDFProcessor
from searcher.VideoProcessor import VideoProcessor
import argparse
import json
import os
import logging
logging.basicConfig(level=logging.WARNING)

'''
def load_mongodb(config):
    """load mongodb from a json file"""
    mongodb_path = config['mongodb_path']
    DBer = DatabaseAccess()
    DBer.import_json_db(db_path=mongodb_path, drop_flag=True)

    print('load_mongodb: Done!')


def connect_remote_mongodb(config, var_file, mongodb_ip, mongodb_port):
    """connect mongodb server from remote"""
    mongodb_service_path = 'mongodb://{0}:{1}'.format(mongodb_ip, mongodb_port)
    mongodb_service_name = config['mongodb_service_name']
    mongodb_collection_name = config['mongodb_collection_name']
    mongodb_increment_beginning_pointer \
        = var_file['mongodb_increment_beginning_pointer']
    DBer = DatabaseAccess(service_path=mongodb_service_path,
                          service_name=mongodb_service_name,
                          collection_name=mongodb_collection_name,
                          increment_beginning_pointer=mongodb_increment_beginning_pointer)

    print('connect_remote_mongodb: Done!')
'''


def init_var_file(var_file_path, var_file):
    """initialize the values in varFile.json"""
    var_file['mongodb_increment_beginning_pointer'] = -1
    var_file['cache_dir_index'] = 1
    var_file['crawler_cache_dir_index'] = 0
    var_file['mongodb_increment_next_pointer'] = -1
    var_file['next_pointer_list'] = []
    with open(var_file_path, 'w', encoding='utf-8') as f:
        json.dump(var_file, f)


def process_pdf(pdf_dir, pdf_ip, pdf_port):
    """process pdfs in the pdf_dir"""
    PDFer = PDFProcessor()
    # pdf_dir = config['pdf_dir']
    PDFer.PDFtoXML(server=pdf_ip, port=pdf_port, pdf_dir=pdf_dir)


def process_video(video_dir):
    """process videos in the video_dir"""
    Videoer = VideoProcessor()
    # video_dir = config['video_dir']
    Videoer.video2text(videos_path=video_dir)


def build_indices(config, var_file_path, var_file, mongodb_ip,
                  mongodb_port, pdf_ip, pdf_port, es_ip, es_port,
                  force_delete=False, connected=False,
                  local_mongo_drop_flag=True
                  ):
    """build ES indices using the current mongodb database"""
    batch_size = config['batch_size']
    cache_dir_index = var_file['cache_dir_index']
    Dper = DataProcess(connected=connected,
                       delete_indices=force_delete,
                       batch_size=batch_size,
                       config=config,
                       var_file=var_file,
                       mongodb_ip=mongodb_ip,
                       mongodb_port=mongodb_port,
                       local_mongo_drop_flag=local_mongo_drop_flag,
                       es_ip=es_ip,
                       es_port=es_port)

    mongodb_increment_next_pointer = \
        Dper.process(pdf_ip=pdf_ip,
                     pdf_port=pdf_port,
                     cache_dir_index=cache_dir_index)

    # update mongodb_increment_next_pointer
    # via the last index in this fetch action
    var_file['mongodb_increment_next_pointer'] = mongodb_increment_next_pointer
    with open(var_file_path, 'w', encoding='utf-8') as f:
        json.dump(var_file, f)

    print('build_indices: Done!')


def update_cache_dir_index(var_file_path, var_file):
    """update cache_dir_index in varFile.json
        1. Check if the corresponding index dir is in the /data/cache/
        2. add 1 to the value of cache_dir_index
    """
    dir_index = var_file['cache_dir_index']
    current_file_path = \
        '/'.join(os.path.split(os.path.realpath(__file__))[0].split('\\'))
    dir_path = os.path.join(current_file_path,
                            'searcher', 'data',
                            'cache', str(dir_index))

    if os.path.exists(dir_path) is not True:
        raise Exception('File not found: ', dir_path)

    var_file['cache_dir_index'] = dir_index + 1
    var_file['mongodb_increment_beginning_pointer'] \
        = var_file['mongodb_increment_next_pointer']
    var_file['next_pointer_list'].append(var_file['mongodb_increment_next_pointer'])
    with open(var_file_path, 'w', encoding='utf-8') as f:
        json.dump(var_file, f)


def test_query(config):
    """check if the elasticsearch can work independently"""
    esclient = ESClient(ip_port='127.0.0.1:9200', delete=False)
    query = {
        "type": 1,
        "top_number": 8192,
        "query_text": {
            "title": "[Oral at NeurIPS 2020] \
                DVERGE: Diversifying Vulnerabilities\
                for Enhanced Robust Generation of Ensembles",
            "authors": "",
            "abstract": "",
            "content": "",
            "year": 2020,
        },
        "operator": ["AND", "", "", "", "AND"]
    }
    rest = esclient.search(query)
    logging.warning('len: ' + str(len(rest)))


if __name__ == '__main__':
    """Usage:
        0. python main.py --mode init_var_file
        1. python main.py --mode process_pdf --pdf_ip PDF_IP\
            --pdf_port PDF_PORT --pdf_dir PDF_DIR
        2. python main.py --mode process_video --video_dir VIDEO_DIR
        3(first). python main.py --mode build_indices_remote --mongodb_ip MONGODB_IP\
            --mongodb_port MONGODB_PORT \
        --pdf_ip PDF_IP --pdf_port PDF_PORT --es_ip ES_IP --es_port ES_PORT\
            --delete_indices 1
        3(next). python main.py --mode build_indices_remote --mongodb_ip MONGODB_IP\
            --mongodb_port MONGODB_PORT \
        --pdf_ip PDF_IP --pdf_port PDF_PORT --es_ip ES_IP --es_port ES_PORT\
            --delete_indices 0
        3_demo(first). python main.py --mode build_indices_local\
            --pdf_ip PDF_IP --pdf_port PDF_PORT \
            --es_ip ES_IP --es_port ES_PORT --delete_indices 1\
            --local_mongo_drop_flag 1
        3_demo(next). python main.py --mode build_indices_local\
            --pdf_ip PDF_IP --pdf_port PDF_PORT \
        --es_ip ES_IP --es_port ES_PORT --delete_indices 0\
            --local_mongo_drop_flag 0
        4. python main.py --mode update_cache_dir_index
        5_test. python main.py --mode test_query
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode",
                        type=str, default='test_query')
    parser.add_argument("--config",
                        type=str, default='./config.json',
                        help='the path to config.json')
    parser.add_argument("--var_file",
                        type=str, default='./varFile.json',
                        help='store variables')
    parser.add_argument("--pdf_ip",
                        type=str, default='localhost',
                        help='ip of grobid server')
    parser.add_argument("--pdf_port",
                        type=str, default='8070',
                        help='port of grobid server')
    parser.add_argument("--pdf_dir",
                        type=str, default='./searcher/data/cache/1/PDFs',
                        help='path to the directory /PDFs')
    parser.add_argument("--mongodb_ip",
                        type=str, default='127.0.0.1',
                        help='ip of mongodb server')
    parser.add_argument("--mongodb_port",
                        type=str, default='27017',
                        help='port of mongodb server')
    parser.add_argument("--delete_indices",
                        type=int, default=0,
                        help='whether to delete es indices')
    parser.add_argument("--local_mongo_drop_flag",
                        type=int, default=1,
                        help='drop local mongodb or not')
    parser.add_argument("--es_ip",
                        type=str, default='127.0.0.1',
                        help='ip of es server')
    parser.add_argument("--es_port",
                        type=str, default='9200',
                        help='ip of es port')
    parser.add_argument("--video_dir",
                        type=str, default='./searcher/data/cache/1/videos',
                        help='path of the director of /videos')
    args = parser.parse_args()

    with open(args.config, 'r', encoding='utf-8') as config_json:
        config = json.load(config_json)
    with open(args.var_file, 'r', encoding='utf-8') as var_file_json:
        var_file = json.load(var_file_json)

    if args.mode == 'init_var_file':
        init_var_file(var_file_path=args.var_file, var_file=var_file)
    elif args.mode == 'process_pdf':
        process_pdf(pdf_dir=args.pdf_dir,
                    pdf_ip=args.pdf_ip,
                    pdf_port=args.pdf_port)
    elif args.mode == 'process_video':
        process_video(video_dir=args.video_dir)
    elif args.mode == 'build_indices_local':
        build_indices(config=config,
                      var_file_path=args.var_file,
                      var_file=var_file,
                      mongodb_ip=args.mongodb_ip,
                      mongodb_port=args.mongodb_port,
                      pdf_ip=args.pdf_ip,
                      pdf_port=args.pdf_port,
                      force_delete=bool(args.delete_indices),
                      connected=False,
                      local_mongo_drop_flag=bool(args.local_mongo_drop_flag),
                      es_ip=args.es_ip,
                      es_port=args.es_port)
    elif args.mode == 'build_indices_remote':
        build_indices(config=config,
                      var_file_path=args.var_file,
                      var_file=var_file,
                      mongodb_ip=args.mongodb_ip,
                      mongodb_port=args.mongodb_port,
                      pdf_ip=args.pdf_ip,
                      pdf_port=args.pdf_port,
                      force_delete=bool(args.delete_indices),
                      connected=True,
                      local_mongo_drop_flag=False,
                      es_ip=args.es_ip,
                      es_port=args.es_port)
    elif args.mode == 'update_cache_dir_index':
        update_cache_dir_index(var_file_path=args.var_file,
                               var_file=var_file)
    elif args.mode == 'test_query':
        test_query(config=config)
    else:
        print('Invaild action!')
