#!/usr/bin/env python
# coding: utf-8
#!/usr/bin/env python
# coding: utf-8
import os
import ee
import geemap
import time
from glob import glob
from tqdm import tqdm
from os.path import basename, join, isfile
from concurrent.futures import ThreadPoolExecutor
import geopandas as gpd
import GoogleEarthEngineDatasets as geed

def s1_median_image(aoi, pol='VH', opass='ASCENDING', idate='2019-01-01', fdate='2022-12-01'):
    """Fetch median Sentinel-1 image for given AOI."""
    collection = ee.ImageCollection("COPERNICUS/S1_GRD") \
        .filterBounds(aoi) \
        .filterDate(idate, fdate) \
        .filter(ee.Filter.eq('instrumentMode', 'IW')) \
        .filter(ee.Filter.listContains('transmitterReceiverPolarisation', pol)) \
        .filter(ee.Filter.eq('resolution_meters', 10))

    size = collection.size().getInfo()
    if size == 0:
        print("No images found with the specified filters.")
        return None
    else:
        image = collection.median().clip(aoi)
        print(f"Bands available: {image.bandNames().getInfo()}")
        image = image.select(['VV', 'VH'])  # Confirm and select only needed bands
        return image

def get_ee_geometry(ig, name):
    """Get EE Rectangle Geometry and output filename."""
    bBox = [
        float(ig.minx.iloc[0]),
        float(ig.miny.iloc[0]),
        float(ig.maxx.iloc[0]),
        float(ig.maxy.iloc[0])
    ]
    fname = basename(ig.location.values[0]).replace('..tif', '.tif')
    fname = fname.replace('.tif', f'_{name}.tif')
    region = ee.Geometry.Rectangle(bBox)
    return region, fname

def gee_download_geemap(image, outpath, scale):
    """Download EE image using geemap export."""
    if isfile(outpath):
        print(f'Already downloaded: {outpath}')
        return
    print(f'Downloading: {outpath}')
    geemap.ee_export_image(image, outpath, scale=scale)

def download_sentinel1(j, g, pol, name, S1tile_path, scale):
    """Single tile Sentinel-1 download worker."""
    ig = g.iloc[[j]]  # Keep it as DataFrame for consistency
    region, fname = get_ee_geometry(ig, name)
    outpath = join(S1tile_path, fname)
    
    if isfile(outpath):
        print(f'Skipping already existing file: {outpath}')
        return

    s1img = s1_median_image(region, pol)
    if s1img:
        gee_download_geemap(s1img, outpath, scale)
    else:
        print(f"No data found for tile {fname}, skipping download.")

def sentinel1_download_tiles(g, pol, name, S1tile_path, scale, cpus=10):
    """Download all Sentinel-1 tiles for a given geopackage."""
    with ThreadPoolExecutor(max_workers=cpus) as executor:
        futures = []
        for j in range(g.shape[0]):
            #if j > 2: break
            futures.append(executor.submit(download_sentinel1, j, g, pol, name, S1tile_path, scale))

def main():
    from uvars import data_download_dir, pathern

    pol = 'VV'
    name = 'S1_VVVH'
    cpus = 30
    scale = 40

    data_dload_dir = os.path.join(data_download_dir, name)
    print('Starting Sentinel-1 download (S1downloadBlock.py)')

    gpkg_files = sorted(glob(pathern), reverse=True)
    print(f"Files found: {len(gpkg_files)}")

    geed.ee_api_initialize(api="high_volume")
    ti = time.perf_counter()

    for i, gfile in enumerate(tqdm(gpkg_files, desc="Processing Files", unit="file")):
        #if i > 100: break #0
        print(gfile)
        g = gpd.read_file(gfile)
        g[['minx', 'miny', 'maxx', 'maxy']] = g.bounds
        tname = os.path.splitext(os.path.basename(gfile))[0]
        S1tile_path = os.path.join(data_dload_dir, tname)
        os.makedirs(S1tile_path, exist_ok=True)

        sentinel1_download_tiles(g, pol, name, S1tile_path, scale, cpus)

    tf = time.perf_counter() - ti
    print(f'RUN.TIME {tf/60:.2f} mins')

if __name__ == "__main__":
    main()




## add the verification of the downloaded files, count the files per tile  [] 
# gpkg_files split by groups and run per grou so as not too overload the systems 
## currenly breaks at 2 