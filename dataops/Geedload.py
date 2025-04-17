#!/usr/bin/env python
# coding: utf-8
import os 
import ee 
import geemap
import time 
from glob import glob
from os.path import basename,join,isfile
from concurrent.futures import ThreadPoolExecutor
import geopandas as gpd 
import GoogleEarthEngineDatasets as geed 

def s1_median_image(aoi,pol='VH', opass='ASCENDING',idate='2019-01-01',fdate='2022-12-01'):
    collection = ee.ImageCollection("COPERNICUS/S1_GRD") \
        .filterBounds(aoi) \
        .filterDate('2022-01-01', '2022-01-31') \
        .filter(ee.Filter.eq('instrumentMode', 'IW')) \
        .filter(ee.Filter.listContains('transmitterReceiverPolarisation', pol))\
        .filter(ee.Filter.eq('resolution_meters',10)) \

    # Check if the collection is not empty
    size = collection.size().getInfo()
    if size == 0:
        print("No images found with the specified filters.")
    else:
        image = collection.median().clip(aoi)
        # Confirm available bands
        print(image.bandNames().getInfo())
        image = image.select(['VV', 'VH'])
        print(image.bandNames().getInfo())
        return image
    
def get_ee_geometry(i, g, name):
    ig = g.iloc[i:i+1,]
    bBox = [float(ig.minx), float(ig.miny), float(ig.maxx), float(ig.maxy)]
    fname = (basename(ig.location.values[0])).replace('..tif', '.tif')
    fname = fname.replace('.tif', f'_{name}.tif')
    region = ee.Geometry.Rectangle(bBox)
    return region, fname


def gee_download_geemap(image,outpath, scale):
    print(outpath)
    if isfile(outpath):
        print('Already downloaded') 
    else:
        geemap.ee_export_image(image, outpath, scale=scale)


def download_sentinel1(i,g,pol,name,S1tile_path,scale):
    region, fname = get_ee_geometry(i, g,name)
    s1img = s1_median_image(region, pol)
    outpath = join(S1tile_path, fname)
    gee_download_geemap(s1img,outpath, scale)


def sentinel1_download_par(i,g,pol,name,tname,S1tile_path,scale,gpkg_files):
    with ThreadPoolExecutor(cpus) as TEX:
        for j in range(g.shape[0]):
            #if j > 2: break
            print(f'{j}/{g.shape[0]} @{tname} :: {i}{len(gpkg_files)}')
            TEX.submit(download_sentinel1,j,g,pol,name,S1tile_path,scale)




def s2_rgb_median_image(aoi, idate='2019-01-01', fdate='2022-12-01'):
    collection = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED") \
        .filterBounds(aoi) \
        .filterDate(idate, fdate) \
        .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 10)) \
        .map(lambda image: image.divide(10000))  # normalize

    if collection.size().getInfo() == 0:
        print("No S2 images found.")
        return None
    else:
        image = collection.median().clip(aoi)
        image = image.select(['B4', 'B3', 'B2'])  # RGB
        print("S2 RGB bands selected:", image.bandNames().getInfo())
        return image


def download_sentinel2(i, g, name, S2tile_path, scale):
    region, fname = get_ee_geometry(i, g, name)
    s2img = s2_rgb_median_image(region)
    if s2img is not None:
        outpath = join(S2tile_path, fname)
        gee_download_geemap(s2img, outpath, scale)


def sentinel2_download_par(i, g, name, tname, S2tile_path, scale, gpkg_files):
    with ThreadPoolExecutor(cpus) as TEX:
        for j in range(g.shape[0]):
            print(f'{j}/{g.shape[0]} @{tname} :: {i}{len(gpkg_files)}')
            TEX.submit(download_sentinel2, j, g, name, S2tile_path, scale)


def process_gpkg_file(i, gfile,outdir,name="S2_RGB"):
    data_dload_dir = os.path.join(outdir,name)
    os.makedirs(data_dload_dir, exist_ok=True)
    g = gpd.read_file(gfile)
    g[['minx', 'miny', 'maxx', 'maxy']] = g.bounds
    tname = os.path.basename(gfile).replace('.gpkg','')

    if name == 'S1_VVVH':
        tile_path = os.path.join(data_dload_dir, tname)
        os.makedirs(tile_path, exist_ok=True)
        sentinel1_download_par(i, g, pol, name, tname, tile_path, scale, gpkg_files)

    elif name == 'S2_RGB':
        tile_path = os.path.join(data_dload_dir, tname)
        os.makedirs(tile_path, exist_ok=True)
        sentinel2_download_par(i, g, name, tname, tile_path, scale, gpkg_files)



### add if statement to download RGB, and other datasets too 
# add if statemes if S1_VVVH, download the below
#if S2_RGB also add the code to download RGB (cloud-free)


cpus = int(os.cpu_count() * 0.75)
scale = 30#15 

pol = 'VV'
name = 'S1_VVVH'

#data_dload_dir = f"/media/ljp238/12TBWolf/ARCHIEVE/GEEDataDownload/{name}" 
pathern = "/home/ljp238/Downloads/DRC_LiDAR_Roy2021/GEEdata/patches/*/*_tindex.gpkg"
gpkg_files = sorted(glob(pathern),reverse=True)
print(gpkg_files)

data_dload_dir = "/media/ljp238/12TBWolf/ARCHIEVE/GEEDataDownload/" 
if __name__ == "__main__":
    geed.ee_api_initialize(api="high_volume")
    ti = time.perf_counter()

    for i, gfile in enumerate(gpkg_files):
       
        process_gpkg_file(i, gfile,data_dload_dir,name="S2_RGB")

    tf = time.perf_counter() - ti 
    print(f'RUN.TIME {tf/60:.2f} mins')





# if __name__ == "__main__":
#     geed.ee_api_initialize(api="high_volume")
#     ti = time.perf_counter()

#     for i in range(len(gpkg_files)):
#         #if i > 0 : break
#         gfile = gpkg_files[i]
#         g = gpd.read_file(gfile)
#         g[['minx','miny','maxx','maxy']] = g.bounds
#         print('gfile', gfile)
#         tname = os.path.basename(gfile).replace('.gpkg','')
#         S1tile_path = os.path.join(data_dload_dir, tname)
#         os.makedirs(S1tile_path, exist_ok=True)
#         sentinel1_download_par(i,g,pol,name,tname,S1tile_path,scale,gpkg_files)
    
#     tf = time.perf_counter() - ti 
#     print(f'RUN.TIME {tf/60} mins')
        
