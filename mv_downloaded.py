import os
import argparse

NUM_WRITE_PROCS = 1
file_name = "raw_content_{}.jsonl.gz"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--download_dir", help="The name of the directory to create and download files to.", required=True)
    parser.add_argument("--output_dir", help="The name of the directory to move files to.", required=True)
    parser.add_argument("--num_write_procs", default=6, type=int)
    args = parser.parse_args()
    NUM_WRITE_PROCS = args.num_write_procs
    
    os.makedirs(args.output_dir, exist_ok=True)
    print(f'Moving files from {args.download_dir} to {args.output_dir}')
    
    already_done = []
    all_indexes = []
    for fn in sorted(os.listdir(args.download_dir)):
        index = fn.replace('raw_content_', '').replace('.jsonl.gz', '')
        # print(index)
        index = int(index)
        all_indexes.append(index)
    for index in all_indexes:
        fn = f"raw_content_{index}.jsonl.gz"
        if index + NUM_WRITE_PROCS in all_indexes:
            print('yes', fn)
            os.rename(os.path.join(args.download_dir, fn), os.path.join(args.output_dir, fn))
        else:
            print('no ', fn)
