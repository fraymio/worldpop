""" Main function to download, process, and upload World Pop age gender rasters. """
import os
import urllib.request
from ftplib import FTP
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from .utils import worldpop_metadata

FTP_URL = "ftp.worldpop.org.uk"
PRODUCT_PATH = "GIS/AgeSex_structures/Global_2000_2020"
S3_BUCKET = "fraym-worldpop"


def build_urls(iso3, year):
    """
    Login to FTP server and build a list of urls for each raster by country and year

    :param iso3 code for the country you want data for

    :param year is the year you want, starts at 2000 and ends at 2020
    """
    ftp = FTP(FTP_URL)
    ftp.login()
    ftp.cwd(f"{PRODUCT_PATH}/{year}")
    urls = ftp.nlst(iso3)
    urls = [os.path.join("ftp://", FTP_URL, PRODUCT_PATH, str(year), x) for x in urls]
    return urls


def download(url, out_dir=None):
    """
    Download a worldpop raster from the FTP server

    :param url to file endpoint
    :type str

    :param out_dir optional path to save file
    :type str, optional
    """
    out_dir = Path(out_dir or "")
    urllib.request.urlretrieve(url, out_dir / os.path.basename(url))


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
