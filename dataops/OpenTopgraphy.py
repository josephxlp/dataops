"""
- SRTMGL3 (SRTM GL3 90m)
- SRTMGL1 (SRTM GL1 30m)
- SRTMGL1_E (SRTM GL1 Ellipsoidal 30m)
- AW3D30 (ALOS World 3D 30m)
- AW3D30_E (ALOS World 3D Ellipsoidal, 30m)
- SRTM15Plus (Global Bathymetry SRTM15+ V2.1 500m)
- NASADEM (NASADEM Global DEM)
- COP30 (Copernicus Global DSM 30m)
- COP90 (Copernicus Global DSM 90m)
- EU_DTM (DTM 30m)
- GEDI_L3 (DTM 1000m)
- GEBCOIceTopo (Global Bathymetry 500m)
- GEBCOSubIceTopo (Global Bathymetry 500m)
"""


import os
import requests
import rasterio
from rasterio.warp import transform_bounds


dem_types = {
    "SRTM GL3 90m": "SRTMGL3",
    "SRTM GL1 30m": "SRTMGL1",
    "SRTM GL1 Ellipsoidal 30m": "SRTMGL1_E",
    "ALOS World 3D 30m": "AW3D30",
    "ALOS World 3D Ellipsoidal, 30m": "AW3D30_E",
    "Global Bathymetry SRTM15+ V2.1 500m": "SRTM15Plus",
    "NASADEM Global DEM": "NASADEM",
    "Copernicus Global DSM 30m": "COP30",
    "Copernicus Global DSM 90m": "COP90",
    "DTM 30m": "EU_DTM",
    "DTM 1000m": "GEDI_L3",
    "Global Bathymetry 500m": "GEBCOIceTopo",
    "Global Bathymetry Sub Ice 500m": "GEBCOSubIceTopo"
}
print('available datasets on globaldem API')
print(dem_types)

def get_WGS84_bounds(rpath):
    with rasterio.open(rpath) as src:
        # Get the bounding box and CRS of the raster
        bounds = src.bounds
        src_crs = src.crs
        
        # Reproject bounds if the CRS is not WGS84
        if src_crs != 'EPSG:4326':
            bounds = transform_bounds(src_crs, 'EPSG:4326', 
                                       bounds.left, bounds.bottom, bounds.right, bounds.top)
        
    # Extract reprojected or original coordinates
    Xmin, Ymin, Xmax, Ymax = bounds
    return Xmin, Ymin, Xmax, Ymax


def download_globaldem(outdir,bbox=None ,rpath=None, varname="SRTMGL3", api_key="api"):
    """
    Downloads DEM data from the OpenTopography API for the bounding box of a given raster file.

    Parameters:
        outdir (str): Directory to save the downloaded DEM file.
        rpath (str): Path to the raster file.
        varname (str): DEM type (default is "SRTMGL3").
        api_key (str): OpenTopography API key.

    Returns:
        None
    """
    # Get bounding box in WGS84
    if rpath is not None:
        print('using get_WGS84_bounds')
        Xmin, Ymin, Xmax, Ymax = get_WGS84_bounds(rpath)

    if bbox is not None:
        print('using bbox')
        Xmin, Ymin, Xmax, Ymax = bbox

    print(f"Extracted Coordinates ::: Xmin = {Xmin}\t Ymin = {Ymin}\t Xmax = {Xmax}\t Ymax = {Ymax}")

    # Construct the API request URL
    url = (
        f"https://portal.opentopography.org/API/globaldem?"
        f"demtype={varname}&south={Ymin}&north={Ymax}&west={Xmin}&east={Xmax}"
        f"&outputFormat=GTiff&API_Key={api_key}"
    )

    # Define the output file path
    output_filename = f"{varname}_{Xmin:.4f}_{Ymin:.4f}_{Xmax:.4f}_{Ymax:.4f}.tif"
    output_path = os.path.join(outdir, output_filename)

    if not os.path.isfile(output_path):
        try:
            # Make the API request
            response = requests.get(url)

            # Handle response codes
            if response.status_code == 200:
                # Save the data locally as a GeoTIFF file
                with open(output_path, 'wb') as f:
                    f.write(response.content)
                print(f"Data downloaded and saved to {output_path}")

            elif response.status_code == 204:
                print("No data available for the specified bounding box.")
            elif response.status_code == 400:
                print("Bad request. Please check the parameters or bounding box coordinates.")
            elif response.status_code == 401:
                print("Unauthorized. Please check your API key.")
            elif response.status_code == 500:
                print("Internal server error. Please try again later.")
            else:
                print(f"Unexpected error: HTTP {response.status_code}")
                print("Response:", response.text)
                print("URL:", url)

        except requests.RequestException as e:
            print(f"Request failed: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")
