"""AWS S3 utility functions

This module has some functions that interact with Amazon S3.
Currently, the main operation supported is uploading bill
pdfs to a bucket.
"""
import logging
from typing import Optional

import boto3

from datafeeds import config


log = logging.getLogger(__name__)


def s3_key_exists(bucket, key):
    """Determine if a key exists in an S3 bucket."""

    if not config.enabled("S3_BILL_UPLOAD"):
        return False

    client = boto3.client("s3")
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except:  # noqa: E722
        return False


def upload_file_to_s3(body, bucket, key, file_display_name=None, content_type=None):
    """Upload a file to s3

    Stores a PDF into a S3 bucket, under the specified key, if that
    key doesn't already exist in the bucket.

    Args:
        body: The contents of the pdf file. Should be a binary file-like
            object (e.g. the result of opening a file in binary mode).
        bucket: The name of the bucket to upload the file into.
        key: The key under which to store the file
        file_display_name: The "original" filename, placed in the
            "content-disposition" metadata of the upload. This
            argument is optional, and if not specified defaults to
            the key name.
        content_type: set as ContentType metadata for the file; defaults to application/pdf

    Returns:
        The name of the key where the file is stored. Should be equal
        to the "key" argument.
    """
    log.debug(
        "S3 Upload Requested: key=%s, bucket=%s, display_name=%s",
        key,
        bucket,
        file_display_name,
    )

    if not config.enabled("S3_BILL_UPLOAD"):
        log.debug("Bill upload disabled, skipping S3 upload.")
        return None

    # see if already fetched/uploaded
    if s3_key_exists(bucket, key):
        log.debug("Key %s already exists in bucket %s.", key, bucket)
        return key

    if file_display_name is None:
        file_display_name = key

    client = boto3.client("s3")
    resp = client.put_object(
        Body=body,
        Bucket=bucket,
        ContentDisposition="inline; filename=%s" % file_display_name,
        ContentType=content_type,
        Key=key,
    )

    log.debug("Attempted S3 upload to %s %s: %s", bucket, key, resp)

    return key


def upload_pdf_to_s3(body, bucket, key, file_display_name=None):
    return upload_file_to_s3(
        body,
        bucket,
        key,
        file_display_name=file_display_name,
        content_type="application/pdf",
    )


def read_file_from_s3(bucket: str, key: str) -> Optional[bytes]:
    """Return the data associated with a single file in S3."""
    client = boto3.client("s3")
    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except:  # noqa: E722
        log.exception("Request to download file from S3 failed.")
        return None

    return response.get("Body").read()


def remove_file_from_s3(bucket: str, key: str) -> None:
    client = boto3.client("s3")
    try:
        client.delete_object(Bucket=bucket, Key=key)
    except:  # noqa: E722
        log.exception("Request to remove file %s/%s from S3 failed.", bucket, key)
