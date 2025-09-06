from pathlib import Path


def upload_dir_to_s3(local_dir: Path, bucket: str, prefix: str = "") -> dict:
    """Upload all files in local_dir into s3://{bucket}/{prefix}/.

    Returns a mapping of local->s3 keys. Requires boto3 and AWS credentials
    available via environment or IAM role.
    """
    try:
        import boto3
    except Exception:
        raise RuntimeError(
            "boto3 is required for S3 uploads. Install with pip install boto3"
        ) from None

    s3 = boto3.client("s3")
    results = {}
    for p in local_dir.glob("*"):
        if p.is_file():
            key = f"{prefix.rstrip('/')}/{p.name}" if prefix else p.name
            s3.upload_file(str(p), bucket, key)
            results[str(p)] = "s3://" + bucket + "/" + key
    return results
