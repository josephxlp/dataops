import os 
import time
import geopandas as gpd
from os.path import basename, join

################################
def tiff2gpkg(tif,figpkg,fogpkg):
    gdal_polygonize(tif, figpkg)
    polygonize_to_cutline(figpkg, fogpkg)

def gdal_polygonize(tif, gpkg):
    if not os.path.isfile(gpkg):
        t1 = time.time()
        cmd_polygonize = f"gdal_polygonize.py -overwrite {tif} -f GPKG {gpkg}"
        os.system(cmd_polygonize)
        t2 = time.time()
        print(f"time cmd_outline:{t2-t1}")
        print(f"Polygon gdal_polygonize saved to {gpkg}")
    else:
        print(f"file areaddy created {gpkg}")
    print('gdal_polygonize')


def polygonize_to_cutline(figpkg, fogpkg):
    if not os.path.isfile(fogpkg):
        t1 = time.time()
        print('reading...')
        gdf = gpd.read_file(figpkg)
        print('dissolving...')
        diss = gdf.dissolve()
        print('writting...')
        diss.to_file(fogpkg, driver="GPKG")
        t2 = time.time()
        print(f"time polygonize_to_cutline:{t2-t1}")
        print(f"Outline file  saved to {fogpkg}")
    else:
        print(f"file areaddy created {fogpkg}")
    print('polygonize_to_cutline')