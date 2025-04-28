import os
import itertools
import pathlib
import numpy as np
import rasterio as rio
from rasterio import windows
from rasterio.transform import Affine
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
#source: GeoTile 

# https://github.com/osgeonepal/geotile/blob/main/geotile/GeoTile.py add others
class GeoTile:
    def __init__(self, path):
        self._read_raster(path)

    def __del__(self):
        self.ds.close()

    def _read_raster(self, path):
        self.path = path
        self.ds = rio.open(path)
        self.meta = self.ds.meta
        self.height = self.meta["height"]
        self.width = self.meta["width"]
        self.meta["crs"] = self.ds.crs

    def _calculate_offset(self, stride_x, stride_y):
        self.stride_x = stride_x
        self.stride_y = stride_y
        X = range(0, self.width, stride_x)
        Y = range(0, self.height, stride_y)
        self.offsets = list(itertools.product(X, Y))

    def _windows_transform_to_affine(self, window_transform):
        a, b, c, d, e, f, _, _, _ = window_transform
        return Affine(a, b, c, d, e, f)

    def _generate_single_tile(self, args):
        idx, col_off, row_off, output_folder, suffix, prefix, save_tiles, save_transform, out_bands, image_format, dtype = args
        window = windows.Window(col_off=col_off, row_off=row_off, width=self.tile_x, height=self.tile_y)
        transform = windows.transform(window, self.ds.transform)
        transform = self._windows_transform_to_affine(transform)
        meta = self.ds.meta.copy()
        nodata = meta["nodata"]
        meta.update({"width": window.width, "height": window.height, "transform": transform})
        out_bands = out_bands or [i + 1 for i in range(self.ds.count)]
        meta.update({"count": len(out_bands)})

        tile = self.ds.read(out_bands, window=window, fill_value=nodata, boundless=True)
        if dtype:
            meta.update({"dtype": dtype})
        else:
            dtype = self.ds.meta["dtype"]
        tile = tile.astype(dtype)

        if save_tiles:
            image_format = image_format or pathlib.Path(self.path).suffix
            suffix = suffix or ""
            prefix = prefix or ""
            tile_name = f"{prefix}{str(idx)}{suffix}{image_format}" if suffix or prefix else f"tile_{col_off}_{row_off}{image_format}"
            tile_path = os.path.join(output_folder, tile_name)

            if save_transform:
                txt_name = f"{prefix}{str(idx)}{suffix}.txt" if suffix or prefix else f"tile_{col_off}_{row_off}.txt"
                txt_path = os.path.join(output_folder, txt_name)
                with open(txt_path, "w") as f:
                    f.write(str(transform.to_gdal()) + "\n")
                    f.write(meta["crs"].to_proj4())

            with rio.open(tile_path, "w", **meta) as outds:
                outds.write(tile)

        return tile, transform

    def generate_tiles(
        self,
        output_folder: Optional[str] = "tiles",
        suffix: Optional[str] = None,
        prefix: Optional[str] = None,
        save_tiles: Optional[bool] = True,
        save_transform: Optional[bool] = False,
        out_bands: Optional[list] = None,
        image_format: Optional[str] = None,
        dtype: Optional[str] = None,
        tile_x: Optional[int] = 256,
        tile_y: Optional[int] = 256,
        stride_x: Optional[int] = 128,
        stride_y: Optional[int] = 128,
        num_workers: Optional[int] = None,
    ):
        self.tile_x = tile_x
        self.tile_y = tile_y
        self._calculate_offset(stride_x, stride_y)
        os.makedirs(output_folder, exist_ok=True) if save_tiles else None
        args_list = [
            (
                i,
                col_off,
                row_off,
                output_folder,
                suffix,
                prefix,
                save_tiles,
                save_transform,
                out_bands,
                image_format,
                dtype,
            )
            for i, (col_off, row_off) in enumerate(self.offsets)
        ]
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            results = list(executor.map(self._generate_single_tile, args_list))
        if not save_tiles:
            self.tile_data = np.array([r[0] for r in results])
            self.tile_data = self.tile_data.astype(dtype or self.ds.meta["dtype"])
            self.tile_data = np.moveaxis(self.tile_data, 1, -1)
            self.window_transform = [r[1] for r in results]
