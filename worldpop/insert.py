""" Main function to insert preprocessed World Pop rasters into the database. """
import os

import boto3
from sqlalchemy import exc

from .db import WorldPop, create_session, raster2pgsql, standardize_tile
from .utils import worldpop_metadata

S3_BUCKET = "fraym-worldpop"


def insert(
    file: str, tile_size: int = 400, dev: bool = False,
):
    """ Insert a World Pop gender/age population raster into database.

    :param file name of raster file
    :type str
    :param tile_size number of pixels in tile, eg. 400 creates 400x400 tiles
    :type int
    :param dev insert into dev or live database
    :type bool

    :rtype None, raster is inserted
    """
    iso3_code, gender, age_lower, age_upper, year = worldpop_metadata(file)

    if age_upper == 0:
        # For the 80 and up group, set upper bound to missing
        age_upper = None

    wp_data = dict(
        iso3_code=iso3_code.upper(),
        year=year,
        gender=gender.lower(),
        age_lower=age_lower,
        age_upper=age_upper,
    )
    db_session = create_session(dev=dev)
    for tile_number, tile in enumerate(raster2pgsql(file, tile_size=tile_size)):
        wp_tile = WorldPop(**wp_data, tile_number=tile_number + 1)
        wp_tile.rast = standardize_tile(tile, dev=dev)
        db_session.add(wp_tile)
        try:
            db_session.commit()
        except exc.IntegrityError as err:
            db_session.rollback()
            raise err
        wp_tile = None


def insert_from_s3(iso3_code: str, year: int, tile_size: int = 400, dev: bool = False):
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
    for obj in s3.Bucket(S3_BUCKET).objects.filter(
        Prefix=f"{year}/{iso3_code.lower()}"
    ):
        local = os.path.basename(obj.key)
        s3.meta.client.download_file(S3_BUCKET, obj.key, local)
        try:
            insert(local, tile_size=tile_size, dev=dev)
        except exc.IntegrityError as err:
            raise err
        finally:
            if os.path.exists(local):
                os.remove(local)
