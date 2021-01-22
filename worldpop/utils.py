""" Utility functions for downloading, processing, and inserting World Pop. """
import re
import uuid
import warnings

import numpy as np
from osgeo import gdal


def worldpop_metadata(file):
    """ Pull metadata from file name. """
    non_standard_bounds = {"0": [0, 1], "1": [1, 4], "80": [80, 0]}
    regex = re.compile(r".*([a-z]{3})_([m|f])_([0-9]{1,2})_([0-9]{4}).*tif")
    iso3, gender, age_lower, year = re.findall(regex, file)[0]
    lower, upper = non_standard_bounds.get(age_lower, (None, None))
    if lower is None and upper is None:
        lower, upper = int(age_lower), int(age_lower) + 4
    return iso3, gender, lower, upper, year


def raster_area(ds):
    """ Get cell area of raster. """
    transform = ds.GetGeoTransform()
    return abs(transform[1] * transform[5])


def tempraster():
    """ Random name with GDAL memory driver"""
    return f"/vsimem/{uuid.uuid4().hex}.tif"


def resample(file):
    """ Resample a WorldPop raster from 100sqm to 1sqkm

    :param file File name of raster data
    :type str
    """
    ds = gdal.Open(file)
    band = ds.GetRasterBand(1)
    transform = ds.GetGeoTransform()
    wkt = ds.GetProjectionRef()
    nodata = band.GetNoDataValue()

    # Convert original population totals to population density rate
    original_area = raster_area(ds)
    rarray = band.ReadAsArray()
    rarray[np.isclose(rarray, nodata)] = np.nan
    # We'll adjust the resampled raster to match the original sum
    orig_sum = np.nansum(rarray)
    rarray /= original_area

    # Create new raster with population density
    rate_ds = gdal.GetDriverByName("MEM").Create(
        "", rarray.shape[1], rarray.shape[0], 1, gdal.GDT_Float32
    )

    rate_ds.SetGeoTransform(transform)
    rate_ds.SetProjection(wkt)

    rate_b = rate_ds.GetRasterBand(1)
    rate_b.SetNoDataValue(nodata)

    rate_b.WriteArray(rarray)
    rate_ds.FlushCache()

    # Resample density to ~1 sqkm
    warp_options = gdal.WarpOptions(
        width=rarray.shape[1] // 10,
        height=rarray.shape[0] // 10,
        dstSRS=wkt,
        dstNodata=nodata,
    )
    res_ds = gdal.Warp(tempraster(), rate_ds, options=warp_options)
    res_ds.FlushCache()

    # Convert density back to population totals
    res_band = res_ds.GetRasterBand(1)
    res_array = res_band.ReadAsArray()
    res_area = raster_area(res_ds)
    res_array[np.isclose(res_array, nodata)] = np.nan
    res_array *= res_area

    # Remove reference to original raster to overwrite
    ds = None
    # Write final resampled population raster
    # Create options to conform to Cloud Optimized Geotiff standard
    cog_options = ["TILED=YES", "COPY_SRC_OVERVIEWS=YES", "COMPRESS=LZW"]
    final_ds = gdal.GetDriverByName("GTiff").Create(
        file, res_band.XSize, res_band.YSize, 1, gdal.GDT_Int16, options=cog_options,
    )
    final_ds.SetGeoTransform(res_ds.GetGeoTransform())
    final_ds.SetProjection(wkt)

    final_b = final_ds.GetRasterBand(1)
    # Original no data value (-99999) can't be stored as Int16
    int16_nodata = -9999
    final_b.SetNoDataValue(int16_nodata)

    res_sum = np.nansum(res_array)
    # Adjust the sum of the resampled raster to match the original sum
    res_array = res_array * orig_sum / res_sum
    # Separate decimal and integer part or the resampled array
    dec_part, res_array = np.modf(res_array)
    int_sum = np.nansum(res_array)
    valid_cells = (~np.isnan(res_array)).sum()
    # The number of cells that need to be rounded up is the difference of the
    # original sum and the portion represented by integers. We'll round up
    # the top X number of cells to match
    adj_percentile = (1 - (orig_sum - int_sum) / valid_cells) * 100
    adj_threshold = np.nanpercentile(dec_part, adj_percentile)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        res_array += (dec_part > adj_threshold).astype("int")

    res_array[np.isnan(res_array)] = int16_nodata
    final_b.WriteArray(res_array.astype("int"))

    final_ds.FlushCache()

    # Close datasets and convert final to bytes
    rate_ds = res_ds = None
