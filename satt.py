import os
import glob
from osgeo import gdal
from osgeo.gdalconst import *
import shutil
import json

folder_name = 'co-challenge/2019-10-15'
folder_path = 'all_tifs'
parent_dir = '/home/atakan/Desktop/satellite_data'

def list_tifs_by_size():
    list_of_files = filter(os.path.isfile, glob.glob(folder_name + '/**/*', recursive=True))

    files_with_size = [(file_path.split("/")[-1], os.stat(file_path).st_size, file_path) for file_path in list_of_files]
    sorted_files_with_size = sorted(files_with_size, key=lambda tup: tup[1],reverse=True)

    print(f"number of files: {len(sorted_files_with_size)}")

    for file_name_displayed, file_size, file_path in sorted_files_with_size:
        print(file_size, '==>', file_name_displayed)

    all_tifs_path = os.path.join(parent_dir,folder_path)
    try:
        os.mkdir(all_tifs_path)
    except Exception as e:
        print(e)

    for file_name_displayed, file_size,file_path in sorted_files_with_size:
        shutil.move(file_path,all_tifs_path)

    path_to_be_removed = os.path.join(parent_dir,folder_name)
    shutil.rmtree(path_to_be_removed)

def mapping_files(tif_file,band_group):
    for b in band_group:
        if b in tif_file:
            new_b = os.path.join(folder_path,b)
            new_f = os.path.join(folder_path,tif_file)
            try:
                shutil.move(new_f,new_b)
            except Exception as e:
                print(f"{new_f} couldn't be moved")
                print(e)

def create_bands(parent_dir,folder_path):
    import threading

    folder_path = os.path.join(parent_dir,folder_path)
    all_tif_files = os.listdir(folder_path)
    band_group = []
    for f in all_tif_files:
        band = "_".join(f.split("_")[4:6])
        band_group.append(band)

    band_group = set(band_group)
    print(f"band group: {band_group}")

    for b in band_group:
        new_b = os.path.join(folder_path,b)
        os.mkdir(new_b)

    for tif_file in all_tif_files:
        t1 = threading.Thread(target=mapping_files,args=[tif_file,band_group])
        t1.start()


def analyse_tif(file_name):
    ds = gdal.Open(file_name, GA_ReadOnly)
    if ds is None:
        print(f'{file_name} not opened')
        return
    
    
    options = gdal.InfoOptions(format='json')
    infos = gdal.Info(ds=ds,options=options)
    ref_system = infos.get('coordinateSystem').get('wkt')[:15].replace('PROJCRS["','')
    size = infos.get('size')
    print(f"Spatial Reference System of the file is: {ref_system}")
    print(f"Raster's pixel size: {size[0]},{size[1]}")

    with open("raster_meta.json","w") as outfile:
        json.dump(infos,outfile)
    

list_tifs_by_size()
create_bands()

file_name = 'all_tifs/B04_10m/patched_sentinel_2_2019-10-15_B04_10m_33_N578_W06_1000cm_roff_104_coff_440.tif'
#analyse_tif(file_name)