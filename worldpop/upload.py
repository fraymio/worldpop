""" Functions to upload World Pop to S3. """
import os

import boto3
from botocore.exceptions import ClientError

from .utils import worldpop_metadata

S3_BUCKET = "fraym-worldpop"


def s3_key(file_name):
    """ Format file name as standard S3 key. """
    basename = os.path.basename(file_name)
    iso3_code, *_age_gender, year = worldpop_metadata(basename)
    prefix = f"{year}/{iso3_code.lower()}"
    return f"{prefix}/{basename}"


def exists_on_s3(key):
    """ Check if file already exists on World Pop S3 bucket. """
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError:
        return False


def upload_to_s3(file, force=False):
    """
    Upload World Pop files to Fraym's S3.

    :param file name of World Pop, must be in the same format as the original World Pop
        files to extract metadata from file name
    :type str

    :param force whether to force the upload overwriting existing file
    :type bool

    :rtype None, file is uploaded
    """
    s3 = boto3.client("s3")

    key = s3_key(file)

    # Skip files that have already been uploaded
    if not force and exists_on_s3(key):
        return
    s3.upload_file(file, S3_BUCKET, key)
