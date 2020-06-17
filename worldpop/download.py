""" Main function to download, process, and upload World Pop age gender rasters. """
import ftplib
import os

import boto3
from botocore.exceptions import ClientError

from .utils import resample_worldpop, worldpop_metadata

FTP_URL = "ftp.worldpop.org.uk"
S3_BUCKET = "fraym-worldpop"


def download_worldpop(iso3_code, year):
    """ Download World Pop age and gender population rasters, resample to 1x1km
    and upload to S3 bucket

    :param iso3_code country code
    :type str

    :param year year of population to download
    :type str or int

    :rtype None, resulting file uploaded to S3
    """
    ftp = ftplib.FTP(FTP_URL)
    ftp.login()
    ftp.cwd("GIS/AgeSex_structures/Global_2000_2020")
    ftp.cwd(str(year))

    for remote in ftp.nlst(iso3_code.upper()):
        if not remote.endswith(".tif"):
            continue
        basename = os.path.basename(remote)

        ftp.retrbinary(f"RETR {remote}", open(basename, "wb").write)
        resample_worldpop(basename)


def upload_worldpop(file, force=False):
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
    basename = os.path.basename(file)

    iso3_code, *_age_gender, year = worldpop_metadata(basename)
    prefix = f"{year}/{iso3_code.lower()}"

    # Skip files that have already been uploaded
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=f"{prefix}/{basename}")
    except ClientError:
        if not force:
            return
    s3.upload_file(file, S3_BUCKET, f"{prefix}/{basename}")

