import os
import geopandas as gpd
import rasterio
from shapely.geometry import box
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# --- Base function to process a single tile ---
def process_single_tile(xgdf, tilename, gpkg_block_dir):
    patches_tile_dir = os.path.join(gpkg_block_dir, tilename)
    os.makedirs(patches_tile_dir, exist_ok=True)
    tile_gpkg = os.path.join(patches_tile_dir, f"{tilename}.gpkg")
    
    if os.path.isfile(tile_gpkg):
        # File already exists, skip
        return "skipped", tile_gpkg
    
    # Otherwise, create the GPKG
    subgdf = xgdf[xgdf['tile_name'] == tilename][['tile_name', 'geometry']]
    subgdf.to_file(tile_gpkg, driver="GPKG")
    return "written", tile_gpkg

# --- Sequential processing with progress bar ---
def processing_inseq(xgdf, utilenames, gpkg_block_dir):
    outputs = []
    for tilename in tqdm(utilenames, desc="Processing tiles (Sequential)"):
        status, output = process_single_tile(xgdf, tilename, gpkg_block_dir)
        outputs.append((status, output))
    return outputs

# --- Parallel processing with progress bar ---
def processing_inpar(xgdf, utilenames, gpkg_block_dir, max_workers=4):
    outputs = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_single_tile, xgdf, tilename, gpkg_block_dir): tilename for tilename in utilenames}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing tiles (Parallel)"):
            status, output = future.result()
            outputs.append((status, output))
    return outputs


# Function to get the bounding box of a tif file
def get_bbox(tif_path):
    import rasterio
    with rasterio.open(tif_path) as src:
        return box(*src.bounds)  # returns the bounding box as a shapely geometry

# Get tile names from global grid given a TIF
def get_tilenames_from_global_grid(tif_path, grid_path):
    g = gpd.read_file(grid_path)
    
    # Get the bounding box for the tif file
    bbox = get_bbox(tif_path)
    
    # Create a new column 'intersects' to check intersection with the bounding box
    g['intersects'] = g['geometry'].intersects(bbox)
    
    # Filter the rows where the geometry intersects with the bounding box
    intersecting_tiles = g[g['intersects']]
    
    # Extract the unique tile names
    unique_tile_names = intersecting_tiles['tile_name'].unique()
    
    # Extract the bounding boxes of the intersecting tiles
    unique_tile_bboxs = [tile.bounds for tile in intersecting_tiles.geometry]
    
    return unique_tile_names, unique_tile_bboxs, intersecting_tiles

# --- NEW FUNCTIONS FOR VECTOR INPUTS ---

# Function to get the bounding box of a vector file
def get_bbox_vector(vector_path):
    gdf = gpd.read_file(vector_path)
    total_bounds = gdf.total_bounds  # [minx, miny, maxx, maxy]
    return box(*total_bounds)

# Get tile names from global grid given a vector file
def get_tilenames_from_global_grid_vector(vector_path, grid_path):
    g = gpd.read_file(grid_path)
    
    # Get the bounding box for the vector file
    bbox = get_bbox_vector(vector_path)
    
    # Check intersection with grid tiles
    g['intersects'] = g['geometry'].intersects(bbox)
    
    # Filter tiles that intersect
    intersecting_tiles = g[g['intersects']]
    
    # Extract the unique tile names
    unique_tile_names = intersecting_tiles['tile_name'].unique()
    
    # Extract the bounding boxes of the intersecting tiles
    unique_tile_bboxs = [tile.bounds for tile in intersecting_tiles.geometry]
    
    return unique_tile_names, unique_tile_bboxs, intersecting_tiles


import os
import numpy as np
import geopandas as gpd
import rasterio
from rasterio.transform import from_origin
from multiprocessing import Pool
from tqdm import tqdm
from typing import List, Tuple

# --- Helper: Get raster properties from vector ---
def vector2raster_properties(vpath: str, epsg: int = 4326, res: float = 1/3600) -> Tuple[int, int, rasterio.transform.Affine]:
    vector = gpd.read_file(vpath)
    bbox = vector.total_bounds  # [minx, miny, maxx, maxy]
    width = int((bbox[2] - bbox[0]) / res) + 1
    height = int((bbox[3] - bbox[1]) / res) + 1
    transform = from_origin(bbox[0], bbox[3], res, res)
    return width, height, transform

# --- Helper: Create a dummy raster ---
def make_dummy_raster(vpath: str, rpath: str, epsg: int = 4326, res: float = 1/3600) -> None:
    width, height, transform = vector2raster_properties(vpath, epsg=epsg, res=res)
    with rasterio.open(
        rpath, 'w', driver='GTiff', height=height, width=width,
        count=1, dtype='uint8', crs=f"EPSG:{epsg}", transform=transform,
        compress='lzw'
    ) as dst:
        random_data = np.random.choice([0, 1], size=(height, width), p=[0.5, 0.5])
        dst.write(random_data.astype('uint8'), 1)

# --- Helper: Process one vector file ---
def process_single_vector(args: Tuple[str, str, int, float]) -> str:
    vpath, geepathes_dir, epsg, res = args
    tname = os.path.splitext(os.path.basename(vpath))[0]
    tname_dir = os.path.join(geepathes_dir, tname)
    os.makedirs(tname_dir, exist_ok=True)
    tname_fn = os.path.join(tname_dir, f"{tname}.tif")

    if not os.path.isfile(tname_fn):
        make_dummy_raster(vpath, tname_fn, epsg=epsg, res=res)
    return tname_fn

# --- Sequential Processing ---
def create_dummy_rasters_inseq(vfiles: List[str], geepathes_dir: str, epsg: int = 4326, res: float = 1/3600) -> List[str]:
    dummy_tifs = []
    for vpath in tqdm(vfiles, desc="Creating dummy rasters (Sequential)"):
        tif_path = process_single_vector((vpath, geepathes_dir, epsg, res))
        dummy_tifs.append(tif_path)
    return dummy_tifs

# --- Parallel Processing (with Pool) ---
def create_dummy_rasters_inpar(vfiles: List[str], geepathes_dir: str, epsg: int = 4326, res: float = 1/3600, num_workers: int = 4) -> List[str]:
    args = [(vpath, geepathes_dir, epsg, res) for vpath in vfiles]
    dummy_tifs = []

    with Pool(processes=num_workers) as pool:
        for tif_path in tqdm(pool.imap_unordered(process_single_vector, args), total=len(vfiles), desc="Creating dummy rasters (Parallel)"):
            dummy_tifs.append(tif_path)
    return dummy_tifs



from utils import GeoTile 

def gdaltindex(gpkg, tifs_dir):
    # do a rasterio as well
    cmd = f"gdaltindex -f GPKG {gpkg} {tifs_dir}/*.tif"
    os.system(cmd)

def patchify_workflow(tifi,ps:int,st:int,cpus:int=20):
    gt = GeoTile(tifi)
    outdir = f"{os.path.dirname(tifi)}/{ps}_{st}"
    os.makedirs(outdir, exist_ok=True)

    gt.generate_tiles(
    output_folder=outdir,  # Folder to save tiles
    prefix="tile_",                # Optional: file name prefix
    suffix=None,                   # Optional: file name suffix
    save_tiles=True,               # Save tiles to disk
    save_transform=True,           # Save geotransform + CRS to txt
    out_bands=[1],                 # Read only the first band @@@
    image_format=".tif",           # Output format
    dtype="float32",               # Force output type
    tile_x=ps,                    # Tile width
    tile_y=ps,                    # Tile height
    stride_x=st,                  # Horizontal stride
    stride_y=st,                  # Vertical stride
    num_workers=cpus)                # Parallel threads (e.g., 8))
    tname = tifi.split('/')[-2]
    gpkg = f"{os.path.dirname(tifi)}/{tname}_tindex.gpkg"
    if not os.path.isfile(gpkg):
        gdaltindex(gpkg, tifs_dir=outdir)