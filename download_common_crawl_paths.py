from os import mkdir, path
from subprocess import run
import argparse
import random

parser = argparse.ArgumentParser(description="Downloads raw Common Crawl WARC files paths.")
parser.add_argument("--snapshots", nargs='+', help="The Common Crawl snapshots to download files from, such as CC-MAIN-2022-33 or CC-MAIN-2022-27. Several can be specified.", required=True)
parser.add_argument("--segment_sampling_ratios", type=float, nargs="+", help="The ratios of each Common Crawl snapshot to use. The higher the ratio, the larger the generated dataset (but also the longer the time that the OLM pipeline runs). You should specify one for each snapshot. For example, if you specify '--snapshots CC-MAIN-2022-33 CC-MAIN-2022-27', then --segment_sampling_ratios could be '0.15 0.11'. This means that 15 percent of the segments from CC-MAIN-2022-33 will uniformly randomly sampled and used, and 11 percent of the segments from CC-MAIN-2022-27 will be uniformly randomly sampled and used.", required=True)
parser.add_argument("--seed", type=int, default=42)
parser.add_argument("--paths_type", default="warc")
parser.add_argument("--paths_name", default="my_warc.paths")
args = parser.parse_args()

random.seed(args.seed)

assert len(args.snapshots) == 1
for index in range(len(args.snapshots)):
    # Download the data for a certian common crawl snapshot
    run(f"wget https://data.commoncrawl.org/crawl-data/{args.snapshots[index]}/{args.paths_type}.paths.gz", shell=True)
    run(f"gzip -d {args.paths_type}.paths.gz", shell=True)
    # paths_name = f"{args.paths_type}-{args.snapshots[index]}-{args.segment_sampling_ratios}.paths"
    paths_name = args.paths_name
    run(f"mv {args.paths_type}.paths {paths_name}", shell=True)
    segments = open(paths_name, "r").readlines()
    kept_segments = []
    for segment in segments:
        if random.random() <= args.segment_sampling_ratios[index]:
            kept_segments.append(segment)
    open(paths_name, "w").writelines(kept_segments)
    print(f'Saving paths file at {paths_name}')
