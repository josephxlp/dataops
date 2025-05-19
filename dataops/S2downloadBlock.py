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

import datetime

def get_current_datetime():
    """
    Returns the current date and time, accurate to the second.

    Returns:
        datetime.datetime: A datetime object representing the current date and time.
    """
    now = datetime.datetime.now()
    return now

def s2_median_image(aoi, idate='2019-01-01', fdate='2022-12-01'):
    """Fetch median Sentinel-2 RGB image for given AOI."""
    collection = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED") \
        .filterBounds(aoi) \
        .filterDate(idate, fdate) \
        .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 20))

    size = collection.size().getInfo()
    if size == 0:
        print("No Sentinel-2 images found for AOI.")
        return None
    image = collection.median().clip(aoi)
    image = image.select(['B4', 'B3', 'B2'])  # RGB bands
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

def download_sentinel2(j, g, name, S2tile_path, scale):
    """Single tile Sentinel-2 download worker."""
    ig = g.iloc[[j]]  # Keep it as DataFrame for consistency
    region, fname = get_ee_geometry(ig, name)
    outpath = join(S2tile_path, fname)

    if isfile(outpath):
        print(f'Skipping existing file: {outpath}')
        return
    s2img = s2_median_image(region)
    if s2img:
        gee_download_geemap(s2img, outpath, scale)
    else:
        print(f"No Sentinel-2 data for tile {fname}, skipping.")

def sentinel2_download_tiles(g, name, S2tile_path, scale, cpus=10):
    """Download all Sentinel-2 tiles for a given geopackage."""
    with ThreadPoolExecutor(max_workers=cpus) as executor:
        futures = []
        for j in range(g.shape[0]):
            futures.append(executor.submit(download_sentinel2, j, g, name, S2tile_path, scale))

def verify_downloads(tile_path, expected_count):
    """Verify the number of downloaded files and log any missing."""
    downloaded = [f for f in os.listdir(tile_path) if f.endswith('.tif')]
    count = len(downloaded)
    print(f"Downloaded {count}/{expected_count} in {tile_path}")
    if count < expected_count:
        missing = expected_count - count
        print(f"WARNING: {missing} tiles missing in {tile_path}")
    return count

def split_gpkg_files(gpkg_list, group_size):
    """Split .gpkg files into groups to avoid overloading."""
    for i in range(0, len(gpkg_list), group_size):
        yield gpkg_list[i:i + group_size]

def main():
    from uvars import data_download_dir, pathern
    t1 = get_current_datetime()
    print(f"starting @{t1}")
    scale = 30
    group_size = 2  # Process gpkg files in small groups

    s2_name = 'S2_RGB'
    s2_path = os.path.join(data_download_dir, s2_name)

    print('Starting Sentinel-2 RGB download')

    gpkg_files = sorted(glob(pathern), reverse=True)
    geed.ee_api_initialize(api="high_volume")
    ti = time.perf_counter()

    for group in tqdm(list(split_gpkg_files(gpkg_files, group_size)), desc="Groups", unit="group"):
        for gfile in group:
            print(f"\nProcessing: {gfile}")
            g = gpd.read_file(gfile)
            g[['minx', 'miny', 'maxx', 'maxy']] = g.bounds
            tname = os.path.splitext(os.path.basename(gfile))[0]

            s2_tile_dir = os.path.join(s2_path, tname)
            os.makedirs(s2_tile_dir, exist_ok=True)

            # Sentinel-2 download
            sentinel2_download_tiles(g, s2_name, s2_tile_dir, scale, cpus=10)
            verify_downloads(s2_tile_dir, len(g))

    tf = time.perf_counter() - ti
    print(f'Finished. Total run time: {tf/60:.2f} minutes')
    
    print(f"starting @{t1}")
    t2 =  get_current_datetime()
    print(f"finishing @{t2}")
    
    t2 = time.time()
    print()
if __name__ == "__main__":
    main()
