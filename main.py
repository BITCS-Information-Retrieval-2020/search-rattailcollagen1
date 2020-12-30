"""Create the index from the data crawled"""

from searcher.DataProcess import DataProcess
from searcher.DatabaseAccess import DatabaseAccess
from searcher.ESClient import ESClient
from searcher.PDFProcessor import PDFProcessor
import argparse
import json
import os
import logging
logging.basicConfig(level=logging.WARNING)


def load_mongodb(config):
    """load mongodb from a json file"""
    mongodb_path = config['mongodb_path']
    DBer = DatabaseAccess()
    DBer.import_json_db(db_path = mongodb_path, drop_flag = True)

    print('load_mongodb: Done!')

def process_pdf(config, pdf_ip, pdf_port):
    """process pdfs in the pdf_dir"""
    PDFer = PDFProcessor()
    pdf_dir = config['pdf_dir']
    PDFer.PDFtoXML(server=pdf_ip, port=pdf_port, pdf_dir=pdf_dir)

def build_indices(config, pdf_ip, pdf_port):
    """build ES indices using the current mongodb database"""
    batch_size = config['batch_size']
    Dper = DataProcess(delete_indices=True, batch_size=batch_size)
    Dper.process(pdf_ip=pdf_ip, pdf_port=pdf_port)

    print('build_indices: Done!')

def test_query(config):
    """check if the elasticsearch can work independently"""
    esclient = ESClient(delete=False)
    query = {
        "type": 1,
        "top_number": 10,
        "query_text": {
            "title": "[Oral at NeurIPS 2020] DVERGE: Diversifying Vulnerabilities for Enhanced Robust Generation of Ensembles",
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default='test_query', type=str)
    parser.add_argument("--config", type=str, default="./config.json", help='the path to config.json')
    parser.add_argument("--pdf_ip", type=str, default='localhost', help='ip of grobid server')
    parser.add_argument("--pdf_port", type=str, default='8070', help='port of grobid server')
    args = parser.parse_args()

    with open(args.config, 'r', encoding='utf-8') as config_json:
        config = json.load(config_json)

    if args.mode == 'load_mongodb':
        load_mongodb(config=config)
    elif args.mode == 'process_pdf':
        process_pdf(config=config, pdf_ip=args.pdf_ip, pdf_port=args.pdf_port)
    elif args.mode == 'build_indices':
        build_indices(config=config, pdf_ip=args.pdf_ip, pdf_port=args.pdf_port)
    elif args.mode == 'test_query':
        test_query(config=config)
    else:
        print('Invaild action!')