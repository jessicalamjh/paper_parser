import argparse
import subprocess
import time
import sys
from datetime import datetime


def run_job(i: int):
    cmd = [
        "uv", "run", "python", "scripts/pmcoa/2-parse-xml-to-jsonl.py",
        "--xml-dir", "data/pmcoa/extracted",
        "--output", f"data/pmcoa/papers-{i}.jsonl",
        "--spacy-model", "en_core_sci_sm",
        "--spacy-batch-size", "32",
        "--spacy-n-process", "1",
        "--spacy-max-length", "4000000",
        "--workers", "10",
        "--pending-paragraphs-max", "100",
        "--split", str(i),
    ]

    print(f"[{datetime.now()}] Starting job for split={i}")
    try:
        result = subprocess.run(cmd, check=True)
        print(f"[{datetime.now()}] Finished split={i} (exit={result.returncode})")
    except subprocess.CalledProcessError as e:
        print(f"[{datetime.now()}] ERROR on split={i}: {e}")
    except KeyboardInterrupt:
        print("\nInterrupted by user. Exiting.")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Run PMCOA parsing jobs hourly over a range.")
    parser.add_argument("--start", type=int, required=True, help="Start of range (inclusive)")
    parser.add_argument("--end", type=int, required=True, help="End of range (inclusive)")
    parser.add_argument("--interval", type=int, default=3600, help="Interval in seconds (default: 3600 = 1h)")

    args = parser.parse_args()

    if args.start > args.end:
        print("Error: start must be <= end")
        sys.exit(1)

    for i in range(args.start, args.end + 1):
        run_job(i)

        # Don't sleep after the last job
        if i != args.end:
            print(f"[{datetime.now()}] Sleeping for {args.interval} seconds...\n")
            try:
                time.sleep(args.interval)
            except KeyboardInterrupt:
                print("\nInterrupted during sleep. Exiting.")
                sys.exit(1)

    print(f"[{datetime.now()}] All jobs completed.")


if __name__ == "__main__":
    main()
