import os
import subprocess
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import geopandas as gpd
import pandas as pd

def modify_all_tilenames(tilenames):
    """
    Modifies each tile name in the list by adding a '0' after the first character.

    Parameters:
    tilenames (list): A list of tile name strings.

    Returns:
    list: The modified list of tile names.
    """
    return [t[0] + '0' + t[1:] for t in tilenames]

def wget_download(url, wdir):
    # Extract the filename from the URL
    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path)

    # Check if the file already exists
    filepath = os.path.join(wdir, filename)
    if os.path.isfile(filepath):
        print(f"{filename} already exists in {wdir}.")
        return

    # Construct the wget command with --wait 0.5
    cmd = f'wget --no-check-certificate --wait 0.5 -P {wdir} {url}'

    # Execute the command
    try:
        subprocess.run(cmd, shell=True, check=True)
        print(f"{filename} downloaded successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while downloading {filename}: {e}")

def parallel_wget(urls_list, wdir):
    if len(urls_list) > 1:
        with ThreadPoolExecutor(max_workers=max(1, os.cpu_count() - 2)) as TPX:
            futures = [TPX.submit(wget_download, url, wdir) for url in urls_list]

            for _ in tqdm(as_completed(futures), total=len(futures), desc="Downloading"):
                pass

        print('parallel_wget completed')
    else:
        if urls_list:
            wget_download(urls_list[0], wdir)
        print('Single download completed')

def filter_urls_by_key(urllist, zipnames):
    urls = [url for url in urllist for zipname in zipnames if zipname in url]
    return list(set(urls))

def tilename2zipfilename(g, tilename):
    match = g[g['tile_name'] == tilename]
    if match.empty:
        raise ValueError(f"Tile name '{tilename}' not found in GeoDataFrame.")
    return match['zipfile_name'].values[0]

def tilenames2zipfilename(g, tilenames):
    zipnames = []
    for tilename in tilenames:
        try:
            zipname = tilename2zipfilename(g, tilename)
            zipnames.append(zipname)
        except ValueError as e:
            print(f"Warning: {e}")
    return list(set(zipnames))
