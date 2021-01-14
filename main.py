"""Create the index from the data crawled"""

from ScienceSearcher.DataProcess import DataProcess
# from ScienceSearcher.DatabaseAccess import DatabaseAccess
from ScienceSearcher.ESClient import ESClient
from ScienceSearcher.PDFProcessor import PDFProcessor
from ScienceSearcher.VideoProcessor import VideoProcessor
import argparse
import json
import os
import logging
logging.basicConfig(level=logging.WARNING)


def process_pdf(pdf_dir, pdf_ip, pdf_port, pdf_n_threads):
    """process pdfs in the pdf_dir"""
    PDFer = PDFProcessor()
    # pdf_dir = config['pdf_dir']
    PDFer.PDFtoXML(server=pdf_ip, port=pdf_port, pdf_dir=pdf_dir, n_threads=pdf_n_threads)


def process_video(video_dir):
    """process videos in the video_dir"""
    Videoer = VideoProcessor()
    # video_dir = config['video_dir']
    Videoer.video2text(videos_path=video_dir)


def build_indices(config, mongodb_service_path, mongodb_service_name,
                  mongodb_collection_name, mongodb_beginning_pointer,
                  mongodb_ending_pointer, pdf_ip, pdf_port, es_ip, es_port,
                  delete_indices, processed_dir, index_name
                  ):
    """build ES indices using the current mongodb database"""
    batch_size = config['batch_size']
    Dper = DataProcess(mongodb_service_path=mongodb_service_path,
                       mongodb_service_name=mongodb_service_name,
                       mongodb_collection_name=mongodb_collection_name,
                       es_ip=es_ip,
                       es_port=es_port,
                       es_index_name=index_name,
                       delete_indices=delete_indices,
                       batch_size=batch_size,
                       mongodb_beginning_pointer=mongodb_beginning_pointer,
                       mongodb_ending_pointer=mongodb_ending_pointer
                       )

    Dper.process(pdf_ip=pdf_ip, pdf_port=pdf_port, processed_dir=processed_dir)

    print('build_indices: Done!')


def test_query(config, index_name):
    """check if the elasticsearch can work independently"""
    esclient = ESClient(ip_port='127.0.0.1:9200', delete=False, index_name=index_name, video_index_name=index_name + '_video')
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
        1. python main.py --mode process_pdf --pdf_ip {0}\
            --pdf_port {1} --pdf_dir {2}
        2. python main.py --mode process_video --video_dir {0}
        3(first). python main.py --mode build_indices_remote --mongodb_service_path {0}\
            --mongodb_service_name {1} --mongodb_collection_name {2} --pdf_ip {3} --pdf_port {4}\
            --es_ip {5} --es_port {6} --delete_indices 1 --processed_dir {7} --index_name {8}
        3(next). python main.py --mode build_indices_remote --mongodb_service_path {0}\
            --mongodb_service_name {1} --mongodb_collection_name {2} --pdf_ip {3} --pdf_port {4}\
            --es_ip {5} --es_port {6} --delete_indices 0 --processed_dir {7} --index_name {8}
        4_test. python main.py --mode test_query
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode",
                        type=str, default='test_query')
    parser.add_argument("--config",
                        type=str, default='./config.json',
                        help='the path to config.json')
    parser.add_argument("--pdf_ip",
                        type=str, default='localhost',
                        help='ip of grobid server')
    parser.add_argument("--pdf_port",
                        type=str, default='8070',
                        help='port of grobid server')
    parser.add_argument("--pdf_dir",
                        type=str, default='./ScienceSearcher/data/cache/demo/1/PDFs',
                        help='path to the directory /PDFs')
    parser.add_argument("--mongodb_service_path",
                        type=str,
                        help='path of the mongodb service')
    parser.add_argument("--mongodb_service_name",
                        type=str,
                        help='the service name of the mongodb')
    parser.add_argument("--mongodb_collection_name",
                        type=str,
                        help='the collection name of mongodb')
    parser.add_argument("--delete_indices",
                        type=int, default=0,
                        help='whether to delete es indices')
    parser.add_argument("--es_ip",
                        type=str, default='127.0.0.1',
                        help='ip of es server')
    parser.add_argument("--es_port",
                        type=str, default='9200',
                        help='ip of es port')
    parser.add_argument("--processed_dir",
                        type=str,
                        help='path to the specific directory: /ScienceSearcher/data/cache/demo/[number]')
    parser.add_argument("--video_dir",
                        type=str, default='./ScienceSearcher/data/cache/demo/1/videos',
                        help='path of the director of /videos')
    parser.add_argument("--index_name",
                        type=str, default='papers')
    args = parser.parse_args()

    with open(args.config, 'r', encoding='utf-8') as config_json:
        config = json.load(config_json)

    if args.mode == 'process_pdf':
        process_pdf(pdf_dir=args.pdf_dir,
                    pdf_ip=args.pdf_ip,
                    pdf_port=args.pdf_port,
                    pdf_n_threads=config['pdf_n_threads'])
    elif args.mode == 'process_video':
        process_video(video_dir=args.video_dir)
    elif args.mode == 'build_indices_remote':
        increment_info_json = os.path.join(args.processed_dir, 'increment_info.json')
        with open(increment_info_json, 'r', encoding='utf-8') as f:
            increment_info = json.load(f)
        mongodb_beginning_pointer = increment_info['begin_id']
        mongodb_ending_pointer = increment_info['end_id']
        build_indices(config=config,
                      mongodb_service_path=args.mongodb_service_path,
                      mongodb_service_name=args.mongodb_service_name,
                      mongodb_collection_name=args.mongodb_collection_name,
                      mongodb_beginning_pointer=mongodb_beginning_pointer,
                      mongodb_ending_pointer=mongodb_ending_pointer,
                      pdf_ip=args.pdf_ip,
                      pdf_port=args.pdf_port,
                      es_ip=args.es_ip,
                      es_port=args.es_port,
                      delete_indices=bool(args.delete_indices),
                      processed_dir=args.processed_dir,
                      index_name=args.index_name)
    elif args.mode == 'test_query':
        test_query(config=config, index_name=args.index_name)
    else:
        print('Invaild action!')
