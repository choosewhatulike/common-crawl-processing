import json
from tqdm import tqdm
import io
from multiprocessing import Pool
import os
import gzip
import jieba
# from flagged_words import flagged_words
# badwords = flagged_words["zh"]

num_workers = 30

def is_bad_doc(doc, badwords):
    count = 0
    for bad_word in badwords:
        bad_word = bad_word.strip()
        if bad_word in doc:
            count += doc.count(bad_word)
            if count > 3:
                return True

    return False

def load_file(dataset_name):
    with gzip.open(dataset_name, 'rb') as fp:
        for line in fp:
            yield line
            
def count_ngrams(text, ngrams_dict):
    for i in range(len(text)):
        for n in range(2, 5):
            gram = tuple(text[i:i+n])
            if len(gram) == n:
                if gram in ngrams_dict[n]:
                    ngrams_dict[n][gram] += 1
                else:
                    ngrams_dict[n][gram] = 1
    return ngrams_dict

def process(item):
    text = item.get('content', '')
    
    # 删除字数量不在100~100000范围内的文档
    # if text is None:
    #     import pdb; pdb.set_trace()
    if len(text) < 100 or len(text) > 100000:
        return False
    
    # 对于行和段落，计算重复行和段落所占比例以及重复行和重复段落字符所占比例，并删除重复比大于30%的文档
    counter = {}
    n_lines = 0
    n_chars = 0
    for line in text.split('\n'):
        line = line.strip()
        if line:
            if line in counter:
                counter[line] += 1
            else:
                counter[line] = 1
            n_lines += 1
            n_chars += len(line)
    n_dup_lines = 0
    n_dup_chars = 0
    for line, n in counter.items():
        if n > 1:
            n_dup_lines += n 
            n_dup_chars += len(line) * n
    dup_line_ratio = n_dup_lines / n_lines
    dup_char_ratio = n_dup_chars / n_chars
    if dup_line_ratio > 0.3 or dup_char_ratio > 0.3:
        return False
    
    # 统计重复的ngram，并删除超过阈值的文档
    ngrams = {n: {} for n in range(2, 5)}
    word_list = []
    for line in text.split('\n'):
        line = line.strip()
        if line:
            words = jieba.lcut(line)
            word_list.extend(words)
            ngrams = count_ngrams(words, ngrams)
    
    n_chars = sum(len(w) for w in word_list)
    
    # for ngram 2-4
    top_ngram_character_fractions = [
        (2, 0.2),
        (3, 0.18),
        (4, 0.16),
    ]
    for n, threshold in top_ngram_character_fractions:
        max_repeat = 0
        max_repeat_w = ''
        for w, repeat in ngrams[n].items():
            if repeat > max_repeat:
                max_repeat = repeat
                max_repeat_w = w
        char_count = sum([len(w) for w in max_repeat_w])
        if char_count * max_repeat / n_chars > threshold:
            return False
        
    # for ngram 5-10
    duplicate_ngram_character_fractions = [
        (5, 0.15),
        (6, 0.14),
        (7, 0.13),
        (8, 0.12),
        (9, 0.11),
        (10, 0.10),
    ]
    for n, threshold in duplicate_ngram_character_fractions:
        fdist = {}
        mark = [0] * len(word_list)
        for i in range(len(word_list) - n + 1):
            bag = tuple(word_list[i: i + n])
            if bag in fdist:
                for j in range(i, i + n):
                    mark[j] = len(word_list[j])
                fdist[bag] += 1
            else:
                fdist[bag] = 1

        if sum(mark) / n_chars > threshold:
            return False
        
    # if is_bad_doc(text, badwords):
    #     return False
    
    return True

def worker(line):
    item = json.loads(line)
    flag = process(item)
    return item, flag

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", required=True)
    parser.add_argument("--output_dir", required=True)
    args = parser.parse_args()
    
    files = os.listdir(args.data_dir)
    pool = Pool(num_workers)
    os.makedirs(args.output_dir)
    for fn in files:
        if not fn.endswith('.gz'):
            continue
        fp = io.BytesIO()
        dataset_name = os.path.join(args.data_dir, fn)
        output_path = os.path.join(args.output_dir, fn)
        print(f'processing {dataset_name}, saving to {output_path}')
        for item, flag in tqdm(pool.imap(worker, load_file(dataset_name))):
            if flag:
                fp.write(json.dumps(item, ensure_ascii=False).encode('utf-8'))
                fp.write(b'\n')
                    
        with gzip.open(output_path, 'wb') as out_fp:
            out_fp.write(fp.getvalue())
