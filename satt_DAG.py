import os
import glob
from osgeo import gdal
from osgeo.gdalconst import *
import shutil
import json

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Variable

import logging
from datetime import datetime, timezone

from satellite_data.raster_mosaic import create_mosaics_in_batch

folder_name = 'co-challenge/2019-10-15'
folder_path = 'all_tifs'
parent_dir = '/home/atakan/Desktop/satellite_data'

def logger(fn):
    from functools import wraps

    @wraps(fn)
    def inner(*args, **kwargs):
        called_at = datetime.now(timezone.utc)
        print(f">>> Running {fn.__name__!r} function. Logged at {called_at}")
        logging.info(f">>> Running {fn.__name__!r} function. Logged at {called_at}")
        to_execute = fn(*args, **kwargs)
        print(f">>> Function: {fn.__name__!r} executed. Logged at {called_at}")
        logging.info(f">>> Function: {fn.__name__!r} executed. Logged at {called_at}")
        return to_execute

    return inner


@logger
def list_tifs_by_size(folder_name,parent_dir,folder_path):
    list_of_files = list(filter(os.path.isfile, glob.glob(os.path.join(parent_dir,folder_name) + '/**/*', recursive=True)))
    logging.info(f"list_of_files {len(list(list_of_files))}")
    files_with_size = [(os.stat(file_path).st_size, file_path) for file_path in list_of_files]
    sorted_files_with_size = sorted(files_with_size, key=lambda tup: tup[1],reverse=True)

    logging.info(f"number of files: {len(sorted_files_with_size)}")

    for file_size, file_path in sorted_files_with_size:
        logging.info(file_size, '==>', file_path)

    all_tifs_path = os.path.join(parent_dir,folder_path)
    try:
        os.mkdir(all_tifs_path)
    except Exception as e:
        logging.info(f"all_tifs_path: {all_tifs_path} not created")
        logging.error(e)

    for file_size,file_path in sorted_files_with_size:
        shutil.move(file_path,all_tifs_path)

    path_to_be_removed = os.path.join(parent_dir,folder_name)
    shutil.rmtree(path_to_be_removed)

def mapping_files(folder_path,tif_file,band_group):
    for b in band_group:
        band = b.split('/')[-1]
        if band and band in tif_file:
            new_f = os.path.join(folder_path,tif_file)
            try:
                shutil.move(new_f,b)
            except Exception as e:
                logging.info(f"{new_f} couldn't be moved")
                logging.info(e)

@logger
def create_bands(parent_dir,folder_path):
    import threading

    folder_path = os.path.join(parent_dir,folder_path)
    all_tif_files = os.listdir(folder_path)
    band_group = []
    for f in all_tif_files:
        band = "_".join(f.split("_")[4:6])
        band = os.path.join(folder_path,band)
        band_group.append(band)

    band_group = set(band_group)
    logging.info(f"band group: {band_group}")

    for b in band_group:
        new_b = os.path.join(folder_path,b)
        os.mkdir(new_b)

    logging.info("first files:")

    for tif_file in all_tif_files:
        t1 = threading.Thread(target=mapping_files,args=[folder_path,tif_file,band_group])
        t1.start()

@logger
def analyse_tif(file_name):
    ds = gdal.Open(file_name, GA_ReadOnly)
    if ds is None:
        logging.info(f'{file_name} not opened')
        return
    
    options = gdal.InfoOptions(format='json')
    infos = gdal.Info(ds=ds,options=options)
    ref_system = infos.get('coordinateSystem').get('wkt')[:15].replace('PROJCRS["','')
    size = infos.get('size')
    logging.info(f"Spatial Reference System of the file is: {ref_system}")
    logging.info(f"Raster's pixel size: {size[0]},{size[1]}")

    with open("raster_meta.json","w") as outfile:
        json.dump(infos,outfile)

@logger   
def create_mosaics(parent_dir,folder_path):
    folder_path = os.path.join(parent_dir,folder_path)
    create_mosaics_in_batch(folder_path)

op_kwargs = {
    "folder_name": 'co-challenge/2019-10-15',
    "parent_dir": '/home/atakan/Desktop/satellite_data',
    "folder_path": 'all_tifs'
}

analyse_kwargs = {
    "file_name": '/home/atakan/Desktop/satellite_data/all_tifs/B04_10m/patched_sentinel_2_2019-10-15_B04_10m_33_N578_W06_1000cm_roff_104_coff_440.tif'
}

args  ={"owner": "Airflow", "start_date":days_ago(0)}
dag = DAG(dag_id="satt_dag",default_args=args,schedule_interval='0 1 * * *')

with dag:
    today = datetime.now()
    log_time = today.strftime("%a_%b_%d_%Y_%X")
    log_file = f'interesting_data_pipeline{log_time}.log'
    logging.basicConfig(filename=log_file,filemode='w', format='[%(levelname)s]: %(message)s', level=logging.INFO)

    listing_tifs_by_size = PythonOperator(task_id='listing_tifs_by_size', python_callable=list_tifs_by_size,op_kwargs=op_kwargs)
    creating_bands = PythonOperator(task_id='creating_bands',python_callable=create_bands,op_kwargs=op_kwargs)
    anlaysing_tif = PythonOperator(task_id='analysing_tig',python_callable=analyse_tif,op_kwargs=analyse_kwargs) #can be excluded for production pipeline
    creating_mosaics = PythonOperator(task_id='creating_mosaics',python_callable=create_mosaics,op_kwargs=op_kwargs)

    listing_tifs_by_size >> creating_bands >> anlaysing_tif >> creating_mosaics

