""" Main function to download, process, and upload World Pop age gender rasters. """
import ftplib
import os

import boto3
from botocore.exceptions import ClientError

from .utils import resample_worldpop, worldpop_metadata
from .insert import insert_worldpop

FTP_URL = "ftp.worldpop.org.uk"
S3_BUCKET = "fraym-worldpop"


def worldpop_pipeline(iso3_code, year, production=True, dev=True):
    """ Download World Pop age and gender population rasters, resample to 1x1km
    and upload to S3 bucket

    :param iso3_code country code
    :type str

    :param year year of population to download
    :type str or int

    :param production whether to insert processed rasters to the production database.
        Make sure rasters have not already been uploaded, there are no unique
        constraints
    :type bool

    :param dev whether to insert processed rasters to the development database. Make
        sure rasters have not already been uploaded, there are no unique constraints
    :type bool

    :rtype None, resulting file uploaded to S3
    """
    s3 = boto3.client("s3")
    ftp = ftplib.FTP(FTP_URL)
    ftp.login()
    ftp.cwd("GIS/AgeSex_structures/Global_2000_2020")
    ftp.cwd(str(year))

    prefix = f"{year}/{iso3_code.lower()}"

    for remote in ftp.nlst(iso3_code.upper()):
        if not remote.endswith(".tif"):
            continue
        basename = os.path.basename(remote)
        # Skip files that have already been uploaded
        try:
            s3.head_object(Bucket=S3_BUCKET, Key=f"{prefix}/{basename}")
            continue
        except ClientError:
            pass
        # Download and resample
        ftp.retrbinary(f"RETR {remote}", open(basename, "wb").write)
        resample_worldpop(basename)
        # Pull out metadata and insert into database(s)
        _file_iso3, gender, age_lower, age_upper, _file_year = worldpop_metadata(
            basename
        )

        if dev:
            insert_worldpop(basename, year, gender, age_lower, age_upper, dev=True)
        if production:
            insert_worldpop(basename, year, gender, age_lower, age_upper, dev=False)

        # Upload to S3 and delete
        s3.upload_file(basename, S3_BUCKET, f"{prefix}/{basename}")
        os.remove(basename)
        ftp.cwd("..")
