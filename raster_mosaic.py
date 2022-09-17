import rasterio
from rasterio.merge import merge
from rasterio.plot import show
import glob
import os


def create_tif_list(band_path,dirpath,tifs='*.tif'):
    band_path = os.path.join(dirpath,band_path)
    all_tifs = os.path.join(band_path,tifs)
    print(all_tifs)
    tifs_list = glob.glob(all_tifs)
    return tifs_list

def create_src_files(tif_list):
    src_files = []
    for tif in tif_list:
        src = rasterio.open(tif)
        src_files.append(src)
    return src_files

def create_mosaic(src_files):
    mosaic, out_trans = merge(src_files)
    #show(mosaic, cmap='terrain')
    return mosaic,out_trans

def update_metadata(src,mosaic,out_trans):
    out_meta = src.meta.copy()
    out_meta.update({
        "driver": "Gtiff",
        "height": mosaic.shape[1],
        "width": mosaic.shape[2],
        "transform": out_trans,
    })
    return out_meta

def write_to_file(out_file,out_meta,mosaic):
    with rasterio.open(out_file,"w",**out_meta) as dest:
        dest.write(mosaic)


def create_mosaic_from_tifs(band,dirpath):
    tif_list = create_tif_list(band,dirpath)
    src_files = create_src_files(tif_list)
    mosaic,out_trans = create_mosaic(src_files)
    out_meta = update_metadata(src_files[0],mosaic,out_trans)
    write_to_file(band+"_mosaic.tif",out_meta,mosaic)


def create_mosaics_in_batch(tifs_path):
    band_paths = os.listdir(tifs_path)    
    for band in band_paths:
        create_mosaic_from_tifs(band,tifs_path)

def main(tifs_path):
    create_mosaics_in_batch(tifs_path)

main("all_tifs")