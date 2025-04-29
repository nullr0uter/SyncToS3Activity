# S3 Sync Tool

A command-line tool to synchronize a local folder with an AWS S3 bucket.

## Features

- Uploads new or changed files to S3.
- Deletes orphaned files from the S3 bucket.
- Compares files using MD5 hashes (ETag in S3).
- Optional dry-run mode.
- Multithreaded upload for improved performance.

## Requirements

- Python 3.7+
- AWS credentials must be configured (via environment, IAM role, or AWS config file).
- Dependencies: `boto3`, `botocore` (see `requirements.txt`)

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
python SyncToS3Activity.py --localFolder /path/to/folder --bucketName my-s3-bucket
```

### Optional Arguments

- `--prefix` – S3 prefix to sync to (defaults to root).
- `--dryRun` – Simulates sync actions without making changes.
- `--threads` – Number of concurrent upload threads (default: 5).

### Example

```bash
python SyncToS3Activity.py \
  --localFolder ./data \
  --bucketName my-bucket-name \
  --prefix backup \
  --dryRun \
  --threads 10
```

## AWS Credentials

The tool requires valid AWS credentials, which can be provided via:

- AWS CLI configuration (`~/.aws/credentials`)
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- IAM Role (if running on an EC2 instance)

If credentials are missing, the script will exit with an appropriate error.

## License

MIT License
