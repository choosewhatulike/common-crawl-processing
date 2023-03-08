# Copyright (c) 2022 Jianbin Chang

import argparse
import json
import gzip
import io
import os

import langdetect
# import langid
import requests
from tqdm import tqdm
from warcio.archiveiterator import ArchiveIterator
from subprocess import run
from bs4 import BeautifulSoup
from flagged_words import flagged_words
from multiprocessing import Process, Manager, current_process

badwords = flagged_words["zh"]
badwords = frozenset(badwords)

CC_DOMAIN = "https://data.commoncrawl.org"

NUM_FETCH_PROC = 30
NUM_WRITE_PROCS = 1
_CONTENT_LANGUAGE = "languages-cld2:"

def parse_metadata(metadata):
    "get the languages info from the metadata of the WARC record"
    for line in metadata.decode('utf-8').split('\n'):
        if line.startswith(_CONTENT_LANGUAGE):
            obj = json.loads(line[len(_CONTENT_LANGUAGE):].strip())
            if obj['reliable']:
                content_lang = [lang['code'] for lang in obj.get('languages', []) if 'code' in lang]
                if len(content_lang):
                    return content_lang
    return []

def parse_wart_headers(rec_headers):
    record_id_prefix = 'WARC-Record-ID'
    record_url_prefix = 'WARC-Target-URI'
    record_time_prefix = 'WARC-Date'
    data = {}
    for key, val in rec_headers.headers:
        if key == record_time_prefix:
            data['timestamp'] = val
        elif key == record_id_prefix:
            data['id'] = val
        elif key == record_url_prefix:
            data['url'] = val
    return data
            

def split_wart_file(wet_file_path):
    page = {}
    pid = current_process()._identity[0]
    resp = requests.get(wet_file_path)
    if resp.status_code != 200:
        raise RuntimeError(f'下载失败 {wet_file_path}')
    content = io.BytesIO(resp.content)
    with gzip.GzipFile(fileobj=content) as fp:
        for record in ArchiveIterator(fp, arc2warc=True):
            if record.rec_type == 'response':
                # if record.http_headers.get_header('Content-Type') == 'text/html':
                if len(page):
                    yield page
                # print(record.rec_headers)
                page = parse_wart_headers(record.rec_headers)
                page['content'] = record.content_stream().read().decode('utf-8', errors='ignore')
            elif record.rec_type == 'metadata':
                if 'content' in page:
                    page['languages'] = parse_metadata(record.content_stream().read())
                    yield page
                    page = {}
    # os.remove(temp_path)

def is_bad_doc(doc, badwords):
    count = 0
    for bad_word in badwords:
        bad_word = bad_word.strip()
        if bad_word in doc:
            count += doc.count(bad_word)
            if count > 3:
                return True

    return False

def write_worker(pid, download_dir, out_queue):
    file_index = 0
    output_path = f'{download_dir}/raw_content_{file_index * NUM_WRITE_PROCS + pid}.jsonl.gz'
    out_fp = gzip.open(output_path, 'wt', encoding='utf-8')
    progress_bar = tqdm(position=pid, desc=f'Write Process {pid}')
    num_items = 0
    while 1:
        page = out_queue.get()
        if page is None:
            break
        out_fp.write(json.dumps(page, ensure_ascii=False))
        out_fp.write('\n')
        progress_bar.update()
        num_items += 1
        if num_items % 50000 == 0:
            out_fp.close()
            file_index += 1
            output_path = f'{download_dir}/raw_content_{file_index * NUM_WRITE_PROCS + pid}.jsonl.gz'
            out_fp = gzip.open(output_path, 'wt', encoding='utf-8')


def process_worker(in_queue, out_queue, path_queue):
    pid = current_process()._identity[0]
    while 1:
        path  = in_queue.get()
        if path is None:
            # out_queue.put(None)
            break
        try:
            for page in tqdm(split_wart_file(path), position=pid+1, desc=f"Process {pid}", disable=True):
                if "languages" not in page:
                    try:
                        soup = BeautifulSoup(page['content'], 'lxml')
                        text = soup.text
                        # lang, prob = langid.classify(text)
                        lang = langdetect.detect(text)
                        if lang in ["zh-cn", "zh-tw"]:
                            lang = "zh"
                    except Exception as e:
                        print(pid, 'line:124', e)
                        continue
                    if lang in ["zh", "en"]:
                        page['languages'] = lang
                    else:
                        continue
                    
                if "zh" not in page['languages'] and "en" not in page['languages']:
                    continue
                else:
                    try:
                        soup = BeautifulSoup(page['content'], 'lxml')
                        text = soup.text
                    except Exception as e:
                        print(pid, 'line:138', e)
                        continue
                    # if is_bad_doc_v2(text, badwords, page['languages']):
                    if is_bad_doc(text, badwords):
                        continue
                    if soup.title is not None:
                        page['title'] = soup.title.text
                        
                out_queue.put(page)
            path_queue.put((path, True))
        except Exception as e:
            print(pid, 'line:149', e)
            path_queue.put((path, False))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--download_dir", help="The name of the directory to create and download WET files to.", required=True)
    parser.add_argument("--watch_dir", default="watch_cc", help="The directory to watch the downloading status")
    parser.add_argument("--paths", default="warc.paths.gz")
    parser.add_argument("--num_write_procs", default=6, type=int)
    parser.add_argument("--num_fetch_procs", default=6, type=int)
    args = parser.parse_args()
    NUM_WRITE_PROCS = args.num_write_procs
    NUM_FETCH_PROC = args.num_fetch_procs
    
    total_paths = []
    with open(args.paths, 'r', encoding='utf-8') as fp:
        total_paths = [f'{CC_DOMAIN}/{line.strip()}' for line in fp.readlines()]
    
    os.makedirs(args.download_dir)
    os.makedirs(args.watch_dir, exist_ok=True)
    file_idx = 0
    output_idx = 0
    
    manager = Manager()
    in_queue = manager.Queue()
    out_queue = manager.Queue()
    path_queue = manager.Queue()
    
    already_done_path = os.path.join(args.watch_dir, 'already_done.paths')
    already_done = []
    if os.path.exists(already_done_path):
        with open(already_done_path, 'r', encoding='utf-8') as fp:
            for line in fp:
                path, status = line.strip().split('\t')
                if status == 'SUCCESS':
                    already_done.append(path)
    already_done = frozenset(already_done)
    
    for path in total_paths:
        if path not in already_done:
            print(path)
            in_queue.put(path)
    print(in_queue.qsize(), len(total_paths), len(already_done))
        
    procs = []
    writers = []
    for i in range(NUM_FETCH_PROC):
        in_queue.put(None)
        p = Process(target=process_worker, args=(in_queue, out_queue, path_queue))
        p.start()
        procs.append(p)
        
    for i in range(NUM_WRITE_PROCS):
        p = Process(target=write_worker, args=(i, args.download_dir, out_queue))
        p.start()
        procs.append(p)
        
    watch_fp = open(f'{args.watch_dir}/complete.paths', 'w', encoding='utf-8')
    
    num_finished = 0
    file_progress_bar = tqdm(total_paths, desc="Finieshed Files")
    while 1:
        if num_finished + len(already_done) >= len(total_paths):
            break
        path, status = path_queue.get()
        if not status:
            watch_fp.write(f'{path}\tFAILED\n')
            watch_fp.flush()
        else:
            watch_fp.write(f'{path}\tSUCCESS\n')
            watch_fp.flush()
        file_progress_bar.update()
        num_finished += 1
    for i in range(NUM_WRITE_PROCS):
        out_queue.put(None)
    for p in writers:
        p.join()

    