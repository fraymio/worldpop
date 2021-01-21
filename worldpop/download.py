""" Main function to download, process, and upload World Pop age gender rasters. """
from ftplib import FTP
import urllib.request
import os

import boto3
from botocore.exceptions import ClientError

from .utils import resample_worldpop, worldpop_metadata

FTP_URL = "ftp.worldpop.org.uk"
FTP_S_URL = "GIS/AgeSex_structures/Global_2000_2020/"
S3_BUCKET = "fraym-worldpop"


def build_urls(iso3, year):
    """
    Login to FTP server and build a list of urls for each raster by country and year

    :param iso3 code for the country you want data for

    :param year is the year you want, starts at 2000 and ends at 2020
    """
    ftp = FTP(FTP_URL)
    ftp.login()
    ftp.cwd(f"{FTP_S_URL}{year}")
    urls = ftp.nlst(iso3)
    urls = [os.path.join("ftp://", FTP_URL, FTP_S_URL, str(year), x) for x in urls]
    return urls


def download_worldpop(url):
    """
    Download a worldpop raster from the FTP server

    :param url to file endpoint
    """
    urllib.request.urlretrieve(url, os.path.basename(url))


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


def remove_worldpop(file):
    """
    Removes a worldpop raster from working directory after
    it is resampled and sent to s3. This avoids bloating your .

    :param file is the processed raster
    """
    if os.path.isfile(file):
        os.remove(file)
    else:
        print(f"Error: {file} not found")
