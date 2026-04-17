import argparse
import glob
import os.path as path
from tqdm import tqdm

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Concatenate splits of PMC OA papers into a single JSONL file."
    )
    parser.add_argument("--data-dir", type=str)
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    input_filepaths = sorted(list(glob.glob(path.join(args.data_dir, "papers-*.jsonl"))))
    output_filepath = path.join(args.data_dir, "papers.jsonl")

    with open(output_filepath, "w") as f:
        for input_filepath in input_filepaths:
            print(f"Processing {input_filepath}")
            with open(input_filepath, "r") as f_in:
                for line in tqdm(f_in):
                    f.write(line)