## World Pop

### Set up

---

Install python dependencies from `requirements.txt`
To upload files to S3, you'll need to be signed into the AWS CLI.
To insert files to the database, install PostGIS with the [`raster2pgsql`](https://postgis.net/docs/using_raster_dataman.html#RT_Raster_Loader) tool.

### Use

---

To download, resample, insert into database(s), and upload to S3 use the `pipeline` function. Make sure no rasters matching those you're processing
have already been added to the database. There are no constraints in the tables to prevent duplicates.

```python
from worldpop import pipeline

iso3_code = "USA"

for year in range(2000, 2021):
    pipeline(iso3_code, year)
```

There are also functions to run part of the pipeline:

- `build_urls` - given a country and year, return endpoints for all rasters
- `download` - download raw data from World Pop
- `resample` - resample to 100m grid to 1km<sup>2</sup>
- `upload_to_s3` - upload processed rasters to S3
- `insert` - insert rasters to database
- `insert_from_s3` - insert rasters to database that have already been uploaded to S3
- `remove` - delete raster from current directory

### Alternative Workflow

```python
import os
import worldpop as wp

iso3_code = "LBR"

for year in range(2000, 2021):
    print(f"\nProcessing {year}: ", end="", flush=True)
    for i, url in enumerate(wp.build_urls(iso3_code, year)):
        print(f"{i+1}..", end="", flush=True)
        out_path = os.path.basename(url)
        wp.download(url)
        wp.resample(out_path)
        wp.upload_to_s3(out_path)
        os.remove(out_path)
    wp.insert_from_s3(iso3_code, year, tile_size=400, dev=True)
```
