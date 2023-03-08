# Common Crawl Processing
This repository demonstrates the proof-of-concept of extracting text data from Common Crawl for pre-training. It constructs a pipeline for downloading, filtering, and preprocessing the common_crawl WARC data.

## Installation
```
conda create -y -n cc_proc python=3 && conda activate cc_proc
pip install -r requirements.txt
```

## Download
1. Download the paths file and specify the size of the dataset. The `segment_sampling_ratios` range from 0 to 1, controlling the size of the Common Crawl to be downloaded. For example, a ratio of 0.01 means that only 1% of the data from the Common Crawl will be downloaded.
```
python download_common_crawl_paths.py --snapshots CC-MAIN-2023-06 --segment_sampling_ratios 0.01 --seed=42
```

2. Download the WARC records based on the paths. 
```
python download_get_text_from_warc.py --download_dir cc_zh_en_downloads --watch_dir watch_downloads --paths my_warc.paths --num_write_procs 6
```
This command downloads and performs some basic cleaning on WARC records:

- It only obtains records that are in Chinese or English.
- It discards records that contain flagged words.
- It saves the records in jsonl format as separate files.

Record Format (each line is a JSON record):
```
{
    "timestamp":"2023-02-06T12:45:31Z",
    "id":"<urn:uuid:xxxxxxx>",
    "url":"https://xxxx.html",
    "content": "HTML contents ....",
    "languages":[
        "zh"
    ],
    "title":"xxxxxx"
}
```

## Extract Text
The processing is pipelined. Once some records are downloaded, you can move them to a folder waiting for preprocessing.
```
python mv_downloaded.py --download_dir cc_zh_en_downloads --output_dir cc_zh_en_need_extract --num_write_procs 6
```

We use [goose](https://github.com/goose3/goose3) to extract text from HTML pages, which yields higher quality text compared to using WET files.
To extract text in Chinese, run:
```
python extract_text_zh_fast.py --data_dir cc_zh_en_need_extract --output_dir cc_zh_text
```

And to extract text in English, run:
```
python extract_text_en_fast.py --data_dir cc_zh_en_need_extract --output_dir cc_en_text
```

## Apply Filter
Most noisy texts are filtered during the extraction step. This step mainly focuses on removing repetition. We apply the filter from MassiveText:
```
python apply_massivetext_filter.py --data_dir cc_zh_text --output_dir cc_filter_zh_text
```
