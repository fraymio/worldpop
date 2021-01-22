# World Pop

## Set up

---

* Install python dependencies from `requirements.txt`
* To upload files to S3, you'll need to be signed into the AWS CLI.
* To insert files to the database, install PostGIS with the [`raster2pgsql`](https://postgis.net/docs/using_raster_dataman.html#RT_Raster_Loader) tool.

## Use

---

The pipeline to download, resample, insert into database(s), and upload to S3 uses the functions listed below.

** Make sure no rasters matching those you're processing have already been added to the database. There are no constraints in the tables to prevent duplicates. **

- `build_urls` - given a country and year, return endpoints for all rasters
- `download` - download raw data from World Pop
- `resample` - resample to 100m grid to 1km<sup>2</sup>
- `upload_to_s3` - upload processed rasters to S3
- `insert_from_s3` - insert rasters to database that have already been uploaded to S3

### An example workflow

```python
import os
import worldpop as wp

iso3_code = "LBN"

for year in range(2000, 2021):
    print(f"\nProcessing {year}: ", end="", flush=True)
    for i, url in enumerate(wp.build_urls(iso3_code, year)):
        print(f"{i+1}..", end="", flush=True)
        out_path = os.path.basename(url)
        key = wp.s3_key(out_path)
        if wp.exists_on_s3(key):
            continue
        wp.download(url)
        wp.resample(out_path)
        wp.upload_to_s3(out_path)
        # Delete your local copy after uploading
        os.remove(out_path)
# Before running any data base functions, check that the data hasn't already been added
wp.insert_from_s3(iso3_code, year, tile_size=400, dev=True)
```
