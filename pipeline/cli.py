import argparse
from pathlib import Path
from pipeline.run_demo import run_demo


def main():
    p = argparse.ArgumentParser(description='Run the ingestion+normalization demo')
    p.add_argument('--mapping', '-m', type=str, help='Path to mapping YAML (optional)')
    p.add_argument('--work-dir', '-w', type=str, help='Work directory to write standardized files (optional)')
    p.add_argument('--s3-bucket', type=str, help='S3 bucket to upload artifacts (optional)')
    p.add_argument('--s3-prefix', type=str, default='', help='S3 prefix (folder) under the bucket')
    args = p.parse_args()

    run_demo(mapping_path=args.mapping, work_dir=args.work_dir, s3_bucket=args.s3_bucket, s3_prefix=args.s3_prefix)


if __name__ == '__main__':
    main()
