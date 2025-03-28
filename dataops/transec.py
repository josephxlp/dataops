import os
import sys 
import numpy as np
import rasterio
from skimage.transform import resize
from catboost import CatBoostRegressor

from uvars import topoxcale_dir 
sys.path.append(topoxcale_dir)
#from topoxcale.mlxcale import mldownxcale
from topoxcale.sagaxcale import gwrdownxcale

def read_raster(file_path):
    """Reads a raster file and returns the array and metadata."""
    with rasterio.open(file_path) as src:
        array = src.read(1)
        profile = src.profile
        return array, profile

def resize_raster(src_array, target_shape):
    """Resizes a raster array to match the target shape using interpolation."""
    resized_array = resize(
        src_array, 
        target_shape, 
        order=1,  # Bilinear interpolation
        mode='constant', 
        anti_aliasing=True
    )
    return resized_array

def preprocess_data(dif_path, dsm_path, s1_path):
    """Reads and processes input raster data."""
    dif, profile = read_raster(dif_path)
    dsm, _ = read_raster(dsm_path)
    
    # Read Sentinel-1 bands
    with rasterio.open(s1_path) as src:
        s1_band1 = src.read(1)
        s1_band2 = src.read(2) if src.count > 1 else src.read(1)  # Handle single-band files
    
    # Resize Sentinel-1 bands to match DSM dimensions
    target_shape = dsm.shape
    s1_band1_resized = resize_raster(s1_band1, target_shape)
    s1_band2_resized = resize_raster(s1_band2, target_shape)
    
    # Create a valid mask based on the difference raster
    valid_mask = (dif > -99) & (dif < 5000)
    
    # Extract valid values
    valid_dsm = dsm[valid_mask]
    valid_s1_band1 = s1_band1_resized[valid_mask]
    valid_s1_band2 = s1_band2_resized[valid_mask]
    valid_dif = dif[valid_mask]
    
    # Combine features
    X_train = np.column_stack((valid_dsm, valid_s1_band1, valid_s1_band2))
    y_train = valid_dif
    
    return X_train, y_train, dsm, s1_band1_resized, s1_band2_resized, profile

def train_model(X_train, y_train):
    """Trains a CatBoost model."""
    model = CatBoostRegressor(iterations=2000, verbose=100)
    model.fit(X_train, y_train)
    return model

def predict_model(model, dsm, s1_band1, s1_band2):
    """Generates predictions for the entire raster grid."""
    all_features = np.column_stack((dsm.flatten(), s1_band1.flatten(), s1_band2.flatten()))
    predictions = model.predict(all_features).reshape(dsm.shape)
    return predictions

def save_raster(output_path, array, profile):
    """Saves a raster file with the specified profile."""
    profile.update(dtype=rasterio.float32, nodata=-9999.0)
    with rasterio.open(output_path, "w", **profile) as dst:
        dst.write(array.astype('float32'), 1)

def transect_augment_data(dif_path, dsm_path, s1_path, output_path, GWR=True):
    # add the ML laler 
    """Main function to process rasters, train a model, and generate predictions."""
    # Step 1: Preprocess data
    X_train, y_train, dsm, s1_band1_resized, s1_band2_resized, profile = preprocess_data(dif_path, dsm_path, s1_path)

    # Step 2: Train model
    model = train_model(X_train, y_train)

    # Step 3: Predict and save results
    predictions = predict_model(model, dsm, s1_band1_resized, s1_band2_resized)
    dsm_pred_diff = dsm - predictions
    save_raster(output_path, dsm_pred_diff, profile)
    print(f"Prediction results saved to {output_path}")
    if GWR:
        sfix2 = "GWR"
        gwr_path = output_path.replace('.tif', f'_{sfix2}.tif')
        gwrdownxcale(xpath=dsm_path, ypath=output_path, opath=gwr_path,oaux=False,epsg_code=4749, clean=False) 


def transec_augment_data_by_npixels():
    pass 
