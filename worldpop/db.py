""" Database related functions and objects for World Pop. """
import re
import shutil
import subprocess

import sqlalchemy as sa
from geoalchemy2 import Raster
from geoalchemy2.functions import GenericFunction
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DB_HOST = "westpark.cm13ryecdchl.us-east-1.rds.amazonaws.com"
DB_USERNAME = "westpark_admin"
DB_PASSWORD = "hifadhiYaMagharibi1962"


def create_session(dev=False):
    """ Create sessionmaker with database connection. """
    if dev:
        db_name = "westpark_development"
    else:
        db_name = "westpark"
    print(f"Connecting to {db_name}")
    conn_string = (
        f"postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/{db_name}"
    )
    engine = sa.create_engine(conn_string)
    sess = sessionmaker(bind=engine)
    return sess()


Base = declarative_base()


class WorldPop(Base):
    """ World Pop table class"""

    __tablename__ = "worldpop"

    # id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    iso3_code = sa.Column(sa.String(3), primary_key=True)
    tile_number = sa.Column(sa.Integer, primary_key=True)
    year = sa.Column(sa.Integer, primary_key=True)
    gender = sa.Column(sa.String(1), primary_key=True)
    age_lower = sa.Column(sa.Integer, primary_key=True)
    age_upper = sa.Column(sa.Integer, nullable=True)
    rast = sa.Column(Raster)


def raster2pgsql(raster, tile_size=400):
    """ *PostGIS must be installed to use `raster2pgsql`*
    :param raster File name used to extract values using raster2pgsql
    """
    if shutil.which("raster2pgsql") is None:
        raise Exception("raster2pgsql must be installed")
    sp_args = [
        "raster2pgsql",
        "-s",
        "4326",
        "-t",
        f"{tile_size}x{tile_size}",
        "-a",
        raster,
        "_table",
    ]
    proc = subprocess.Popen(sp_args, stdout=subprocess.PIPE, bufsize=-1)
    rasthex = re.compile(r".*VALUES.*\('(.*)'::raster\);")
    while True:
        line = proc.stdout.readline().decode().rstrip()
        if not line:
            proc.kill()
            break
        elif line.startswith("INSERT INTO"):
            try:
                raster_value = re.findall(rasthex, line)[0]
                yield raster_value  # + "::raster"
            except IndexError:
                print(f"Other than one raster value found")
                continue


# Resampling function and args are used to align raster pieces before
class ST_Resample(GenericFunction):
    """ Resample function """

    name = "ST_Resample"
    type = Raster


class ST_MapAlgebra(GenericFunction):
    """ Map Algebra function """

    name = "ST_MapAlgebra"
    type = Raster


def standardize_tile(tile, dev=False):
    """ Wrap a tile in the PostGIS resample function with the same arguments
    used for all fraym rasters to ensure they line up correctly. """
    # Arguments to ST_Resample
    resample_args = (
        0.00833333329986236,  # scalex
        -0.00833333329986236,  # scaley
        0,  # -17.543749915,  # gridx
        0,  # 37.560416718,  # gridy
        0,  # skewx
        0,  # skewy
        "NearestNeighbor",  # algorithm
    )
    map_algebra_args = ("32BSI", "[rast]")  # 32 bit signed int  # column expression
    tile = ST_Resample(tile, *resample_args)
    if not dev:
        tile = ST_MapAlgebra(tile, *map_algebra_args)
    return tile

