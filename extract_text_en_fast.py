# Copyright (c) 2022 Jianbin Chang

import argparse
import json
import gzip
import os

from tqdm import tqdm
from multiprocessing import Process, Queue, current_process
from goose3 import Goose
from goose3.text import StopWords
from utils import Writer

g = Goose({'stopwords_class': StopWords, 'parser_class': 'lxml', 'enable_image_fetching': False})

NUM_FETCH_PROC = 40
# NUM_WRITE_PROCS = 2

def process_goose(item):
    text = item.get('content', '')
    try:
        article = g.extract(raw_html=text)
    except Exception as e:
        print(e)
        item['content'] = ''
        return item
    item['content'] = article.cleaned_text
    return item

def worker(in_queue, out_queue, args):
    while 1:
        path = in_queue.get()
        if path is None:
            break
        try:
            with gzip.open(path, 'rb') as fp:
                for line in fp:
                    item = json.loads(line)
                    if 'en' in item['languages'] and 'zh' not in item['languages']:
                        item = process_goose(item)
                        out_queue.put(item)
            status = "SUCCESS"
        except Exception as e:
            print(e)
            status = "FAILED"
        with open(os.path.join(args.output_dir, 'already_done.paths'), 'a', encoding='utf-8') as fp:
            fn = os.path.basename(path)
            fp.write(f'{fn}\t{status}\n')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", help="The name of the directory to create and download WET files to.", required=True)
    parser.add_argument("--output_dir", default="watch_cc", help="The directory where temporary files are stored. They are deleted when this script completes. Default is .tmp_download_common_crawl.")
    args = parser.parse_args()
    
    procs = []
    in_queue = Queue()
    out_queue = Queue()
    
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
        already_done = frozenset()
    else:
        already_done_path = os.path.join(args.output_dir, 'already_done.paths')
        already_done = []
        if os.path.exists(already_done_path):
            with open(already_done_path, 'r', encoding='utf-8') as fp:
                for line in fp:
                    path, status = line.strip().split('\t')
                    if status == 'SUCCESS':
                        already_done.append(path)
        already_done = frozenset(already_done)
    
    for fn in os.listdir(args.data_dir):
        if fn not in already_done:
            in_queue.put(os.path.join(args.data_dir, fn))
        
    for i in range(NUM_FETCH_PROC):
        in_queue.put(None)
        p = Process(target=worker, args=(in_queue, out_queue, args))
        p.start()
        procs.append(p)

    progress_bar = tqdm(desc='extract text with goose')
    start_index = len([fn for fn in os.listdir(args.output_dir) if fn.endswith('.gz')])
    print('start_index', start_index)
    writer = Writer(args.output_dir, prefix='text_content', offset=start_index, max_items=1000000)
    while 1:
        try:
            item = out_queue.get(timeout=10)
            writer.write_line(json.dumps(item, ensure_ascii=False))
            progress_bar.update()
        except Exception as e:
            print(e)
            if out_queue.empty() and not any([p.is_alive() for p in procs]):
                break
            