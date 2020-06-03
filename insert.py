""" Main function to insert preprocessed World Pop rasters into the database. """
from typing import Optional

import os
import re

import boto3

from .db import create_session, raster2pgsql, standardize_tile, WorldPop
from .utils import worldpop_metadata

S3_BUCKET = "fraym-worldpop"


def insert_worldpop(
    file: str,
    year: int,
    gender: str,
    age_lower: int,
    age_upper: Optional[int] = None,
    tile_size: int = 400,
    dev: bool = False,
):
    """ Insert a World Pop gender/age population raster into database.

    :param file name of raster file
    :type str

    :param year year of raster data
    :type int
    :param gender either 'm' or 'f'
    :type str
    :param age_lower lower age of population
    :type int
    :param age_upper upper age of population
    :type int

    :param tile_size number of pixels in tile, eg. 400 creates 400x400 tiles
    :type int
    :param dev insert into dev or live database
    :type bool

    :rtype None, raster is inserted
    """
    db_session = create_session(dev=dev)
    for tile in raster2pgsql(file, tile_size=tile_size):
        wp_tile = WorldPop(
            year=year, gender=gender.lower(), age_lower=age_lower, age_upper=age_upper,
        )
        wp_tile.rast = standardize_tile(tile, dev=dev)
        db_session.add(wp_tile)
        try:
            db_session.commit()
        except Exception as err:
            db_session.rollback()
            raise Exception from err
        wp_tile = None


def insert_worldpop_from_s3(
    iso3_code: str, year: int, tile_size: int = 400, dev: bool = False
):
    """
    :param iso3_code country code
    :type str

    :param year year of World Pop
    :type int

    :param tile_size size of tiles to be created from raster, eg. 400 will
        create a 400x400 tiles
    :type int

    :param dev insert into development or live database
    :type bool

    :rtype None, raster is inserted
    """

    s3 = boto3.resource("s3")
    pattern = re.compile(r"^[0-9]{4}/[a-z]{3}/.*.tif$")
    for obj in s3.Bucket(S3_BUCKET).objects.iterator():
        insert = re.findall(pattern, obj.key)
        if not insert:
            continue
        local = os.path.basename(obj.key)
        file_iso3, gender, age_lower, age_upper, file_year = worldpop_metadata(local)
        if int(file_year) != year or file_iso3.upper() != iso3_code.upper():
            continue
        if age_upper == 0:
            # For the 80 and up group, set upper bound to missing
            age_upper = None
        s3.meta.client.download_file(S3_BUCKET, obj.key, local)
        insert_worldpop(
            local, year, gender, age_lower, age_upper, tile_size=tile_size, dev=dev,
        )
        try:
            os.remove(local)
        except (FileNotFoundError, PermissionError):
            pass
