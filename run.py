"""Update all of the information incrementally"""
from time import sleep
import argparse
import os
import json


def check_dir_updated(dir_idx, cache_name):
    """check if the /ScienceSearcher/cache/[number] dir is updated"""
    cur_file_path = '/'.join(os.path.split(os.path.realpath(__file__))[0].split('\\'))
    cache_dir = cur_file_path + '/ScienceSearcher/data/cache/' + cache_name
    increment_info_json = os.path.join(cache_dir, str(dir_idx), 'updated.json')

    if os.path.exists(increment_info_json):
        with open(increment_info_json, 'r', encoding='utf-8') as f:
            increment_info = json.load(f)
            if increment_info['updated'] == 0:
                return False
            else:
                return True
    else:
        return False


def set_processed_or_not(begin_dir_index, end_dir_index, set_updated_or_not, cache_name):
    """To set which directory to be unchanged/changed"""
    begin_idx = begin_dir_index
    end_idx = end_dir_index
    cur_file_path = '/'.join(os.path.split(os.path.realpath(__file__))[0].split('\\'))
    cache_dir = cur_file_path + '/ScienceSearcher/data/cache/' + cache_name
    cache_files = os.listdir(cache_dir)
    cache_subdirs = []
    for item in cache_files:
        item_path = os.path.join(cache_dir, item)
        if os.path.isdir(item_path):
            item = int(item)
            cache_subdirs.append(item)

    # filter all of the subdirs need to be processed
    cache_subdirs_tmp = []
    for item in cache_subdirs:
        if (begin_idx == -1 or item >= begin_idx) and (end_idx == -1 or item <= end_idx):
            cache_subdirs_tmp.append(item)

    # set the correponding value of 'updated' in increment_info.json to 0
    cache_subdirs = cache_subdirs_tmp
    for item in cache_subdirs:
        increment_info_json = os.path.join(cache_dir, str(item), 'updated.json')
        cur_dict = {}
        cur_dict['updated'] = set_updated_or_not
        with open(increment_info_json, 'w', encoding='utf-8') as f:
            json.dump(cur_dict, f)


def find_unprocessed_dir(cache_name):
    """find all the unprocessed dirs"""
    cur_file_path = '/'.join(os.path.split(os.path.realpath(__file__))[0].split('\\'))
    cache_dir = cur_file_path + '/ScienceSearcher/data/cache/' + cache_name
    cache_files = os.listdir(cache_dir)
    cache_subdirs = []

    for item in cache_files:
        item_path = os.path.join(cache_dir, item)
        if os.path.isdir(item_path) and not check_dir_updated(dir_idx=item, cache_name=cache_name):
            item = int(item)
            cache_subdirs.append(item)
            set_processed_or_not(begin_dir_index=item, end_dir_index=item, set_updated_or_not=0, cache_name=cache_name)

    return cache_subdirs


def process(args, specific_dir_list=None):
    """tell function 'recovery' which directory need to be processed """
    """check if there is new directory in /ScienceSearcher/data/cache/"""
    # 遍历/ScienceSearcher/data/cache/下的当前所有带标号的文件夹
    cur_file_path = '/'.join(os.path.split(os.path.realpath(__file__))[0].split('\\'))
    cache_dir = cur_file_path + '/ScienceSearcher/data/cache/' + args.cache_name
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    cache_files = os.listdir(cache_dir)
    cache_subdirs = []
    from_scratch = True

    # the following if condition is just for the two crawler groups
    # it will be deleted later
    """Will be deleted: begin"""
    if specific_dir_list is not None:
        recover(args=args, cache_subdirs=specific_dir_list, from_scratch=True)
        print('Finish loading mongodb, name: {0}, dirs: {1}',
              args.es_index_name + ' : ' + args.es_index_name + '_video',
              specific_dir_list)
        return
    """Will be deleted: end"""

    # whether to set all of the directory as unprocessed
    for item in cache_files:
        item_path = os.path.join(cache_dir, item)
        if os.path.isdir(item_path):
            item = int(item)
            cache_subdirs.append(item)
            set_processed_or_not(begin_dir_index=item, end_dir_index=item, set_updated_or_not=0, cache_name=args.cache_name)

    recover(args=args, cache_subdirs=cache_subdirs, from_scratch=from_scratch)
    for item in cache_subdirs:
        from_scratch = False
        set_processed_or_not(begin_dir_index=item, end_dir_index=item, set_updated_or_not=1, cache_name=args.cache_name)

    while True:
        sleep(32)
        print('[!] Starting update...')
        new_cache_subdirs = find_unprocessed_dir(cache_name=args.cache_name)
        recover(args=args, cache_subdirs=new_cache_subdirs, from_scratch=from_scratch)
        for item in new_cache_subdirs:
            from_scratch = False
            set_processed_or_not(begin_dir_index=item, end_dir_index=item, set_updated_or_not=1, cache_name=args.cache_name)
        if new_cache_subdirs != []:
            print('[!] Finish update dirs:{0}'.format(new_cache_subdirs))
        else:
            print('[!] Nothin to update!')


def recover(args, cache_subdirs, from_scratch):
    """Recover information if the local es system crashed"""

    cur_file_path = '/'.join(os.path.split(os.path.realpath(__file__))[0].split('\\'))
    cache_dir = cur_file_path + '/ScienceSearcher/data/cache/' + args.cache_name

    # 对于遍历到的每一个文件夹，首先调用PDFProcessor和VideoProcessor来处理这里的所有pdfs和videos
    for item in cache_subdirs:
        subdir = os.path.join(cache_dir, str(item))

        pdf_dir = os.path.join(subdir, 'PDFs')
        cmd = 'python main.py --mode process_pdf --pdf_ip {0} --pdf_port {1} --pdf_dir {2} \
                '.format(args.pdf_ip, args.pdf_port, pdf_dir)
        os.system(cmd)
        print('cmd: ', cmd)

        video_dir = os.path.join(subdir, 'videos')
        cmd = 'python main.py --mode process_video --video_dir {0}'.format(video_dir)
        os.system(cmd)
        print('cmd: ', cmd)

    # 先把from_scratch设置成true，然后进行第一遍更新indices
    # 然后把from_scratch设置为false，从而处理之后的更新
        if from_scratch:
            from_scratch = False
            cmd = 'python main.py --mode build_indices_remote --mongodb_service_path {0}\
                --mongodb_service_name {1} --mongodb_collection_name {2} --pdf_ip {3} --pdf_port {4}\
                --es_ip {5} --es_port {6} --delete_indices 1 --processed_dir {7} --index_name {8}\
                '.format(args.mongodb_service_path, args.mongodb_service_name, args.mongodb_collection_name,
                         args.pdf_ip, args.pdf_port,
                         args.es_ip, args.es_port,
                         subdir, args.es_index_name
                         )
            os.system(cmd)
            print('cmd: ', cmd)
        else:
            cmd = 'python main.py --mode build_indices_remote --mongodb_service_path {0}\
                --mongodb_service_name {1} --mongodb_collection_name {2} --pdf_ip {3} --pdf_port {4}\
                --es_ip {5} --es_port {6} --delete_indices 0 --processed_dir {7} --index_name {8}\
                '.format(args.mongodb_service_path, args.mongodb_service_name, args.mongodb_collection_name,
                         args.pdf_ip, args.pdf_port,
                         args.es_ip, args.es_port,
                         subdir, args.es_index_name
                         )
            os.system(cmd)
            print('cmd: ', cmd)


if __name__ == '__main__':
    """Usage:
            process:
                1. python run.py --mode process --pdf_ip PDF_IP --pdf_port PDF_PORT\
                    --mongodb_service_path SERVICE_PATH --mongodb_service_name SERVICE_NAME\
                    --mongodb_collection_name COLLECTION_NAME --es_ip ES_IP --es_port ES_PORT
            from_crawler:
                1. python run.py --mode init
                2. python run.py --mode demo --pdf_ip PDF_IP \
                    --pdf_port PDF_PORT --es_ip ES_IP \
                    --es_port ES_PORT --sleep_time SLEEP_TIME
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode",
                        type=str, default='process')
    parser.add_argument("--pdf_ip",
                        type=str, default='localhost',
                        help='ip of grobid server')
    parser.add_argument("--pdf_port",
                        type=str, default='8070',
                        help='port of grobid server')
    parser.add_argument("--mongodb_service_path",
                        type=str,
                        help='path of the mongodb service')
    parser.add_argument("--mongodb_service_name",
                        type=str,
                        help='the service name of the mongodb')
    parser.add_argument("--mongodb_collection_name",
                        type=str,
                        help='the collection name of mongodb')
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
    parser.add_argument("--begin_dir_index",
                        type=int, default=-1,
                        help='beginning index of the directory in /ScienceSearcher/data/cache/[cache_name]')
    parser.add_argument("--end_dir_index",
                        type=int, default=-1,
                        help='end index of the directory in /ScienceSearcher/data/cache/[cache_name]')
    parser.add_argument("--specific_dir_list",
                        type=str, default='2,3,4,5,6',
                        help='end index of the directory in /ScienceSearcher/data/cache/[cache_name]')
    parser.add_argument("--es_index_name",
                        type=str, default='papers',
                        help='the name of es index')
    parser.add_argument("--cache_name",
                        type=str, default='demo',
                        help='the name of cache directory: /ScienceSearcher/data/cache/[cache_name]')

    args = parser.parse_args()
    with open(args.config, 'r', encoding='utf-8') as config_json:
        config = json.load(config_json)

    if args.mode == 'process':
        process(args=args)
    elif args.mode == 'from_crawler':
        specific_dir_list = [int(n) for n in args.specific_dir_list.split(',')]
        process(args=args, specific_dir_list=specific_dir_list)
    else:
        raise Exception('[!] Unrecogized action: ', args.mode)
