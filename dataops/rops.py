### use magic tools to make into a python script 
import os 
from glob import glob
import subprocess
import geopandas as gpd

import rasterio
from rasterio.mask import mask
from shapely.geometry import box  
from rasterio.merge import merge


def merge_tifs(tif_files, output_path):

    if not os.path.isfile(output_path):
        return output_path
    # Open all TIFF files
    src_files = [rasterio.open(f) for f in tif_files]
    
    # Merge the rasters
    merged_array, merged_transform = merge(src_files)
    
    # Use metadata from the first file
    out_meta = src_files[0].meta.copy()
    out_meta.update({
        "driver": "GTiff",
        "height": merged_array.shape[1],
        "width": merged_array.shape[2],
        "transform": merged_transform
    })
    
    # Write merged raster
    with rasterio.open(output_path, "w", **out_meta) as dest:
        dest.write(merged_array)
    
    # Close files
    for src in src_files:
        src.close()

def clip_raster_by_vector_cutline(raster_fn, out_fn, bbox_fn):
    pass 

def clip_raster_by_vector_bbox(raster_fn, out_fn, bbox_fn):

    if not os.path.isfile(out_fn):
        bbox = gpd.read_file(bbox_fn)
        with rasterio.open(raster_fn) as src:
            
            bbox = bbox.to_crs(src.crs)
            raster_bounds = box(*src.bounds)  # Convert bounds to a Shapely Polygon
            raster_gdf = gpd.GeoDataFrame(geometry=[raster_bounds], crs=src.crs)
            
            # Check if bbox overlaps with raster bounds
            if not bbox.intersects(raster_gdf).any():
                raise ValueError(f"No overlap between bbox and raster {raster_fn}")
            
            # Clip raster using bbox geometry
            out_image, out_transform = mask(src, bbox.geometry, crop=True)
            out_meta = src.meta
            out_meta.update({
                "driver": "GTiff",
                "height": out_image.shape[1],
                "width": out_image.shape[2],
                "transform": out_transform
            })
        
        # Save clipped raster
        with rasterio.open(out_fn, "w", **out_meta) as dest:
            dest.write(out_image)


def reproject_files_seq(tiff_files, reproj_dir,epsgcode):
    #epsgcode = 4326 4979
    os.makedirs(reproj_dir, exist_ok=True)
    reprojected_files = []
    for tiff_file in tiff_files:
        # Construct the output file path for the reprojected file
        base_name = os.path.splitext(os.path.basename(tiff_file))[0]
        reproj_tiff = os.path.join(reproj_dir, f"{base_name}_reproj.tif")
        gdalreproject(fi=tiff_file, fo=reproj_tiff, epsgcode=epsgcode)
        reprojected_files.append(reproj_tiff)
    return reprojected_files

def reproject_files_par():
    pass 

def gdalreproject(fi, fo, res=1/3600, epsgcode=4749):
    """
    #4749 #4326
    Reprojects a raster file using gdalwarp.

    Parameters:
        fi (str): Input file path.
        fo (str): Output file path.
        res (float): Resolution for both x and y (default: 1/3600 degrees).
        epsgcode (int): EPSG code for the target projection (default: 4326).

    Returns:
        None
    """
    xres = yres = res  # Set x and y resolution to the same value by default

    # Check if the output file already exists
    if not os.path.isfile(fo):
        # Construct the gdalwarp command
        cmd_reproject = [
            "gdalwarp",
            "-t_srs", f"EPSG:{epsgcode}",  # Target spatial reference system
            "-tr", str(xres), str(yres),  # Resolution for x and y
            fi,  # Input file
            fo   # Output file
        ]

        try:
            # Run the gdalwarp command
            subprocess.run(cmd_reproject, check=True)
            print(f"Reprojected {fi} -> \n{fo}")
        except subprocess.CalledProcessError as e:
            print(f"Error reprojecting {fi}: {e}")
            print(f"Command attempted: {' '.join(cmd_reproject)}")
    else:
        print(f"Output file {fo} already exists. Skipping reprojection.")

# def list2txt(txt, files):
#     with open(txt, "w") as f:
#         f.write("\n".join(files))



def list2txt(txt, files):
    with open(txt, "w") as f:
        for fi in files:
            f.write(fi+'\n')

def gdalbuildvrt(vrt, txt, epsgcode):
    if not os.path.isfile(vrt):
        cmd_vrt = [
        "gdalbuildvrt",
        "-a_srs", f"EPSG:{epsgcode}",    # Assign the specified EPSG code
        "-input_file_list", txt,         # Input file list (text file)
        vrt                              # Output VRT file
        ]

        try:
            # Run the GDAL command to create the VRT
            subprocess.run(cmd_vrt, check=True)
            print(f"VRT file created successfully: {vrt}")
        except subprocess.CalledProcessError as e:
            print(f"Error creating VRT file: {e}")


            