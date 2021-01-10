"""Update all of the information incrementally"""
from time import sleep
import argparse
import os
import json


def init_variables(args):
    os.system('python main.py --mode init_var_file')


def process(args, config):

    while True:
        with open(args.var_file, 'r', encoding='utf-8') as var_json:
            variables = json.load(var_json)
        # nothing to update
        if variables['crawler_cache_dir_index'] < variables['cache_dir_index']:
            print('Nothing to update!')
            sleep(args.sleep_time)


def process_demo(args, config):
    demo_from_scratch_flag = True

    while True:
        with open(args.var_file, 'r', encoding='utf-8') as var_json:
            variables = json.load(var_json)
        # nothing to update
        if variables['crawler_cache_dir_index'] < variables['cache_dir_index']:
            print('Nothing to update!')
            sleep(args.sleep_time)
        # invalid update step
        elif variables['crawler_cache_dir_index'] \
                > variables['cache_dir_index']:
            raise Exception('[!] The value of crawler_cache_dir_index \
                            cannot be higher than cache_dir_index.')
        else:  # something to update
            print('Starting updating...')
            pdf_dir = config['pdf_dir'].format(
                variables['crawler_cache_dir_index'])

            cmd = 'python main.py --mode process_pdf --pdf_ip {0} --pdf_port {1} --pdf_dir {2} \
                '.format(args.pdf_ip, args.pdf_port, pdf_dir)
            os.system(cmd)
            print('cmd: ', cmd)

            video_dir = config['video_dir'].format(
                variables['crawler_cache_dir_index'])
            cmd = 'python main.py --mode process_video --video_dir {0}\
                '.format(video_dir)
            os.system(cmd)
            print('cmd: ', cmd)

            if demo_from_scratch_flag:
                demo_from_scratch_flag = False
                cmd = 'python main.py --mode build_indices_local --pdf_ip {0} --pdf_port {1} \
                    --es_ip {2} --es_port {3} --delete_indices 1 \
                    --local_mongo_drop_flag 1\
                    '.format(args.pdf_ip, args.pdf_port,
                             args.es_ip, args.es_port
                             )
                os.system(cmd)
                print('cmd: ', cmd)
            else:
                cmd = 'python main.py --mode build_indices_local --pdf_ip {0} --pdf_port {1} \
                    --es_ip {2} --es_port {3} --delete_indices 0 \
                    --local_mongo_drop_flag 0\
                    '.format(args.pdf_ip, args.pdf_port,
                             args.es_ip, args.es_port
                             )
                os.system(cmd)
                print('cmd: ', cmd)

            cmd = 'python main.py --mode update_cache_dir_index'
            os.system(cmd)
            print('cmd: ', cmd)
            print('Successfully update dir: [{0}] !'.format(
                variables['crawler_cache_dir_index']))


if __name__ == '__main__':
    """Usage:
            demo:
                1. python run.py --mode init
                2. python run.py --mode demo --pdf_ip PDF_IP \
                    --pdf_port PDF_PORT --es_ip ES_IP \
                    --es_port ES_PORT --sleep_time SLEEP_TIME
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode",
                        type=str, default='demo')
    parser.add_argument("--pdf_ip",
                        type=str, default='localhost',
                        help='ip of grobid server')
    parser.add_argument("--pdf_port",
                        type=str, default='8070',
                        help='port of grobid server')
    parser.add_argument("--mongodb_ip",
                        type=str, default='127.0.0.1',
                        help='ip of mongodb server')
    parser.add_argument("--mongodb_port",
                        type=str, default='27017',
                        help='port of mongodb server')
    parser.add_argument("--es_ip",
                        type=str, default='127.0.0.1',
                        help='ip of es server')
    parser.add_argument("--es_port",
                        type=str, default='9200',
                        help='ip of es port')
    parser.add_argument("--sleep_time",
                        type=int, default='32',
                        help='update interval')
    parser.add_argument("--config",
                        type=str, default='./config.json',
                        help='path to config file')
    parser.add_argument("--var_file",
                        type=str, default='./varFile.json',
                        help='path to varFile.json')

    args = parser.parse_args()
    with open(args.config, 'r', encoding='utf-8') as config_json:
        config = json.load(config_json)

    if args.mode == 'init':
        init_variables(args=args)
    elif args.mode == 'demo':
        process_demo(args=args, config=config)
    elif args.mode == 'remote':
        process(args=args, config=config)
    else:
        raise Exception('[!] Unrecogized action: ', args.mode)
