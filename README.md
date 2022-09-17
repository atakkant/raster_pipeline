# raster_pipeline
a repo for a simple pipeline managing raster data created with python and the modules GDAL and rasterio

satt.py runs standalone for collecting rasters from subfolders of co-challenge and moving them to all_tifs folder and groups them according to their bands
I used multithreading inside the creating_bands method because it is doing a I/O operation and multithreading can make it faster

raster_mosaic.py creates a mosaic for every band folder in all_tifs folder and saves them seperately with the band names

satt_DAG.py is a demonstrative python file for creating the DAGS for Airflow. This file contains all the operations inside satt.py and raster_mosaic.py
Scheduling and ordering the operations is included.
For the purpose of transfering the data between tasks there are a couple of options. First one is writing necessary data to a txt file at the end of one task.
Next task opens that txt file and reads the input and continue the operations
Second choice is using XCOMs which is a little bit more complicated but a standard way for managing the transfer of data between tasks.

