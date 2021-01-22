""" Main function to download, process, and upload World Pop age gender rasters. """
import os
import urllib.request
from ftplib import FTP
from pathlib import Path

FTP_URL = "ftp.worldpop.org.uk"
PRODUCT_PATH = "GIS/AgeSex_structures/Global_2000_2020"


def build_urls(iso3, year):
    """
    Login to FTP server and build a list of urls for each raster by country and year

    :param iso3 code for the country you want data for

    :param year is the year you want, starts at 2000 and ends at 2020
    """
    ftp = FTP(FTP_URL)
    ftp.login()
    ftp.cwd(f"{PRODUCT_PATH}/{year}")

    urls = []
    for f in ftp.nlst(iso3.upper()):
        if f.endswith(".tif"):
            urls.append(os.path.join("ftp://", FTP_URL, PRODUCT_PATH, str(year), f))
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
