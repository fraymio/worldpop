## World Pop

### Set up

---

Install python dependencies from `requirements.txt`
To upload files to S3, you'll need to be signed into the AWS CLI.
To insert files to the database, install PostGIS with the [`raster2pgsql`](https://postgis.net/docs/using_raster_dataman.html#RT_Raster_Loader) tool.

### Use

---

To download, resample, insert into database(s), and upload to S3 use the `worldpop_pipeline` function. Make sure no rasters matching those you're processing
have already been added to the database. There are no constraints in the tables to prevent duplicates.

```python
from worldpop import worldpop_pipeline

iso3_code = "USA"

for year in range(2000, 2021):
    worldpop_pipeline(iso3_code, year)
```

There are also functions to run part of the pipeline:

- `build-urls` - Given a country and year, return endpoints for all rasters
- `download_worldpop` - download raw data from World Pop
- `resample_worldpop` - Resample fro 100m grid to 1km<sup>2</sup>
- `upload_worldpop` - upload processed rasters to S3
- `insert_worldpop` - insert rasters to database
- `insert_worldpop_from_s3` - insert rasters to database that have already been uploaded to S3
- `remove_worldpop` - Deletes raster from current directory

### Alternative Workflow

```python
from worldpop.download import(
    build_urls
    download_worldpop
    upload_worldpop
    remove_worldpop
)
from worldpop.insert import insert_worldpop_from_s3
from worldpop.utils import resample_worldpop

urls = []
iso3_code = "YEM"

for year in range(2000, 2021):
    urls.extend(build_urls(iso3_code, year))
    for url in urls:
        download_worldpop(url)
        resample_worldpop(os.path.basename(url))
        upload_worldpop(os.path.basename(url))
        insert_worldpop_from_s3(iso3_code, year, tile_size=400, dev=True)
        remove_wpop(os.path.basename(url))
```
