import os
import ee 
import geemap
import time 
import pandas as pd
import geopandas as gpd 
from os.path import basename,join,isfile 
from glob import glob
from concurrent.futures import ThreadPoolExecutor
from x import eeproject, gee_highvolume_api 



def ee_api_initialize(api="high_volume"):
    if api =="high_volume":
        try:
            ee.Initialize(project=eeproject,opt_url=gee_highvolume_api)
        except:
            ee.Authenticate()
            ee.Initialize(project=eeproject,opt_url=gee_highvolume_api)
    elif api == "low_volume":
        try:
            ee.Initialize(project=eeproject,opt_url=gee_highvolume_api)
        except:
            ee.Authenticate()
            ee.Initialize(project=eeproject,opt_url=gee_highvolume_api)


def create_dataframe_from_tif_files(base_path):

    data = {}
    for root, dirs, files in os.walk(base_path):
        relative_path = os.path.relpath(root, base_path)   
        if relative_path == ".":
            continue
        
        if relative_path not in data:
            data[relative_path] = []
        
        for file in files:
            if file.endswith('.tif'):
                full_path = os.path.join(root, file)
                data[relative_path].append(full_path)

    df = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in data.items()]))
    return df

def get_ee_geometry(i, g, name):
    ig = g.iloc[i:i+1,]
    bBox = [float(ig.minx), float(ig.miny), float(ig.maxx), float(ig.maxy)]
    fname = (basename(ig.location.values[0])).replace('..tif', '.tif')
    fname = fname.replace('.tif', f'_{name}.tif')
    region = ee.Geometry.Rectangle(bBox)
    return region, fname

def get_S1_image(aoi,pol='VH', opass='ASCENDING',idate='2019-01-01',fdate='2022-12-01'):
    sentinel1 = ee.ImageCollection('COPERNICUS/S1_GRD') \
    .filter(ee.Filter.eq('instrumentMode','IW')) \
    .filterDate(idate,fdate).filter(ee.Filter.listContains('transmitterReceiverPolarisation', pol)) \
    .filter(ee.Filter.eq('orbitProperties_pass',opass)) \
    .filter(ee.Filter.eq('resolution_meters',10)) \
    .filterBounds(aoi)\
   
    s1img = ee.Image(sentinel1.median().clip(aoi))
    s1img = s1img.select(['VV','VH'])
    return s1img

def download_sentinel1(i,g,pol,name,S1tile_path,scale):
    region, fname = get_ee_geometry(i, g,name)
    s1img = get_S1_image(region, pol)
    outpath = join(S1tile_path, fname)
    gee_download_geemap(s1img,outpath, scale)


def ee_clip_mosaic_roi(dobject,roi):
    mosaic = dobject.mosaic()
    mosaiclip = mosaic.clip(roi)
    return mosaiclip

def getDEM_files(roi):
    glo30  = ee.ImageCollection("COPERNICUS/DEM/GLO30").filterBounds(roi)
    wbm = ee_clip_mosaic_roi(glo30.select('WBM'),roi)
    hem = ee_clip_mosaic_roi(glo30.select('HEM'),roi)
    flm = ee_clip_mosaic_roi(glo30.select('FLM'),roi)
    edm = ee_clip_mosaic_roi(glo30.select('EDM'),roi)
    dem = ee_clip_mosaic_roi(glo30.select('DEM'),roi)
    return dem,edm,flm,hem,wbm


def download_sentinel2(i,g,name,S2tile_path,band_codes,scale):
    region, fname = get_ee_geometry_s2(i, g,name)
    rgb = get_S2median(region,band_codes)
    outpath = join(S2tile_path, fname)
    gee_download_geemap(rgb,outpath, scale)
    #time.sleep(0.5)

def get_ee_geometry_s2(i, g, name):
    ig = g.iloc[i:i+1,]
    # Calculate the bounding box from the geometry
    bBox = ig.geometry.bounds.iloc[0].tolist()  # [minx, miny, maxx, maxy]
    fname = (basename(ig.location.values[0]))
    fname = fname.replace('.tif', f'_{name}.tif')
    fname = fname.replace('..tif', f'_{name}.tif')
    region = ee.Geometry.Rectangle(bBox)
    return region, fname

# ENDVI: Enhanced Normalized Difference Vegetation Index.
# NDVI: Normalized Difference Vegetation Index

# ANDWI: Augmented Normalized Difference Water Index.
# NDWI: Normalized Difference Water Index.

# NBAI: Normalized Built-up Area Index.
# UI: Urban Index

def get_S2median(region,band_codes,CLOUD_FILTER=30):
    s2coll = ee.ImageCollection('COPERNICUS/S2_HARMONIZED') \
             .filterBounds(region) \
             .filterDate('2021', '2022') \
             .filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', CLOUD_FILTER))
    

    sentinel2_masked = s2coll.map(mask_clouds)
    #rgb_bands = ['B4', 'B3', 'B2','B8']
    rgb = sentinel2_masked.select(band_codes).median().clip(region)
    return rgb 


def mask_clouds(image):
    qa = image.select('QA60')
    cloudBitMask = int(2**10)
    cirrusBitMask = int(2**11)
    mask = qa.bitwiseAnd(cloudBitMask).eq(0) \
        .And(qa.bitwiseAnd(cirrusBitMask).eq(0))
    return image.updateMask(mask).divide(10000)

def gee_download_geemap(image,outpath, scale):
    print(outpath)
    if isfile(outpath):
        print('Already downloaded') 
    else:
        geemap.ee_export_image(image, outpath, scale=scale)



def get_tilename(subg):
    #minx, miny, maxx, maxy  = g.total_bounds
    minx, miny, maxx, maxy  = subg.total_bounds
    print(minx, miny)
    try:
        minx = int(round(minx,1))
        miny = int(round(miny,1))
        maxx = int(maxx)
        maxy = int(maxy)
    except:
        minx = float(minx)
        miny = float(miny)
        maxx = float(maxx)
        maxy = float(maxy)
    print(minx, miny)
    lat_direction = 'N' if miny >= 0 else 'S'
    lon_direction = 'E' if minx >= 0 else 'W'
    identifier = f'{lat_direction}{abs(miny)}_{lon_direction}{abs(minx)}'
    print(identifier)
    return identifier

def initialize_gee_highvolume_api():
    try:
        ee.Initialize(
        project='ee-josephluntadilap',
        opt_url='https://earthengine-highvolume.googleapis.com')
    except:
        ee.Authenticate()
        ee.Initialize(
        project='ee-josephluntadilap',
        opt_url='https://earthengine-highvolume.googleapis.com')


def download_sentinel2_indices(i,g,name,S2tile_path,scale):
    region, fname = get_ee_geometry_s2(i, g,name)
    #rgb = get_S2median(region,band_codes)
    rgb = get_S2median_indices(region, CLOUD_FILTER=30)
    outpath = join(S2tile_path, fname)
    gee_download_geemap(rgb,outpath, scale)
    #time.sleep(0.5)



def get_S2median_indices(region, CLOUD_FILTER=30):
    """
    # https://developers.google.com/earth-engine/datasets/catalog/COPERNICUS_S2_SR_HARMONIZED
    Generate median composite from Sentinel-2 imagery and compute additional indices.
    
    Parameters:
    - region: ee.Geometry, the area of interest.
    - CLOUD_FILTER: int, maximum cloud cover percentage to include in the imagery.

    Returns:
    - ee.Image: Sentinel-2 median composite including RGB, NIR, and additional indices.
    """
    # Import Sentinel-2 dataset and filter it
    s2coll = ee.ImageCollection('COPERNICUS/S2_HARMONIZED') \
        .filterBounds(region) \
        .filterDate('2015-01-01', '2022-01-01') \
        .filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', CLOUD_FILTER))
    
    # Apply cloud masking
    sentinel2_masked = s2coll.map(mask_clouds)
    
    # Compute median composite and clip to region
    composite = sentinel2_masked.select(['B4', 'B3', 'B2', 'B8', 'B11']).median().clip(region)
    
    # Compute indices
    ndvi = composite.expression(
        'float((NIR - RED) / (NIR + RED))',
        {'NIR': composite.select('B8'), 'RED': composite.select('B4')}
    ).rename('NDVI')

    endvi = composite.expression(
        'float((NIR + GREEN - 2 * RED) / (NIR + GREEN + 2 * RED))',
        {'NIR': composite.select('B8'), 'GREEN': composite.select('B3'), 'RED': composite.select('B4')}
    ).rename('ENDVI')

    ndwi = composite.expression(
        'float((GREEN - NIR) / (GREEN + NIR))',
        {'GREEN': composite.select('B3'), 'NIR': composite.select('B8')}
    ).rename('NDWI')

    andwi = composite.expression(
        'float((GREEN - SWIR1) / (GREEN + SWIR1))',
        {'GREEN': composite.select('B3'), 'SWIR1': composite.select('B11')}
    ).rename('ANDWI')

    nbai = composite.expression(
        'float((SWIR1 - RED) / (SWIR1 + RED))',
        {'SWIR1': composite.select('B11'), 'RED': composite.select('B4')}
    ).rename('NBAI')

    ui = composite.expression(
        'float((SWIR1 - NIR) / (SWIR1 + NIR))',
        {'SWIR1': composite.select('B11'), 'NIR': composite.select('B8')}
    ).rename('UI')

    # Combine RGB, NIR, and indices
    result = composite.addBands([ndvi, endvi, ndwi, andwi, nbai, ui])
    return result


def download_sentinel1_by_gpkg(gpkg_files, sentinel1_dpath,
                               scale = 30,cpus_pct=0.5):
    initialize_gee_highvolume_api()
    cpus = int(os.cpu_count() * cpus_pct)
    name = 'S1_VVVH'
    pol = 'VV'
    #gpkg_files = sorted(glob(patches_pattern),reverse=True)
    #if __name__ == '__main__':
    ti = time.perf_counter()
    for i in range(len(gpkg_files)):
        #if i > 0 : break
        gfile = gpkg_files[i]
        g = gpd.read_file(gfile)
        g[['minx','miny','maxx','maxy']] = g.bounds
        print('gfile', gfile)
        tname = os.path.basename(gfile).replace('.gpkg','')
        S1tile_path = os.path.join(sentinel1_dpath, tname)
        os.makedirs(S1tile_path, exist_ok=True)
        for j in range(g.shape[0]):
            print(f'{j}/{g.shape[0]} @{tname} :: {i}{len(gpkg_files)}')
            download_sentinel1(j,g,pol,name,S1tile_path,scale)

        # with ThreadPoolExecutor(cpus) as TEX:
        #     for j in range(g.shape[0]):
        #         #if j > 2: break
        #         print(f'{j}/{g.shape[0]} @{tname} :: {i}{len(gpkg_files)}')
        #         TEX.submit(
        #             download_sentinel1,j,g,pol,name,S1tile_path,scale)

    tf = time.perf_counter() - ti 
    print(f'RUN.TIME {tf/60} mins')

