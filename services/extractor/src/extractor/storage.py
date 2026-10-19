from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


class S3Store:
    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket: str):
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="garage",
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5, "mode": "standard"}),
        )

    def stored_sha(self, key: str) -> str | None:
        try:
            head = self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as err:
            if err.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return None
            raise
        return head["Metadata"].get("sha256")

    def put(self, key: str, path: Path, metadata: dict[str, str]) -> None:
        self.client.upload_file(
            str(path),
            self.bucket,
            key,
            ExtraArgs={"Metadata": metadata, "ContentType": "application/zip"},
        )
