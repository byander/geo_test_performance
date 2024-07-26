## Description

Benchmark test for executi on of some spatial analysis using different solutions.

### Install the dependencies

For the execution of some scripts, you must first create a Python environment and install the
packages.

### Install dependencies

I usually use venv of Python, but use it according to your preferences.

1. Install Python 3.11.x ou above;
2. Create a virtual environment. Ex.: `python -m venv venv`
3. Activate the virtual environment. Ex.: `source\bin\activate`
4. Install the dependencies: `pip intall -r requirements.txt`

### Source data

I downloaded the source data from MapBiomas Alerta and SICAR and removed invalid geometries
manually.

### Instructions for execution

All the Python scripts you need to send the file path with an extension in the command line.   
The parameters there are:

```text
-car = File path with extension of shapefile from CAR
-mp = File path with extension of shapefile from MapBiomas
-r = Folder to save the Geopackage file
```

Example:

`python some_script.py -car=D:\CAR_AREA_IMOVEL_PR.shp -mp=D:\alerts_with_intersections_Apenas_valido.shp -r=D:\Resultados`

**Note**:
For the [process_pyqgis.py](process_pyqgis.py) script, you must change the parameters in
the [run_script_qgis.bat](run_script_qgis.bat) file.

For the following scripts you need a valid license of ArcGIS Pro:   
[process_arcgis_01.py](process_arcgis_01.py)  
[process_arcgis_02.py](process_arcgis_02.py)

For execution of the scripts created with arcpy, you must execute using the directory Python folder
from the default environments from ArcGIS Pro folder installation.
Ex.: `C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe process_arcgis_01.py`

### Results obtained

Bellow, I shared the results obtained in different machines.

**Machine configuration**

1. PC Desktop 1:  
   CPU: Ryzen 7 5800X 3.8 GHz (8 Cores / 16 Threads)
   RAM: 32 GB  
   SSD: 2 TB NVME M2 Gen 4     
   OS: Windows 10   
   Python: 3.11.1

2. PC Desktop 2 (Kit Xeon by China):  
   CPU: Xeon E5 2697AV4 2.6 GHz (16 Cores / 32 Threads)
   RAM: 32 GB  
   SSD: 1 TB NVME M2 Gen 4 (but the Mobo recognize only Gen 3)   
   OS: Linux Pop!_OS 22.04   
   Python: 3.12.4

3. Orange Pi 3B:
   CPU: Rockchip RK3566 (4 Cores / 4 Threads) ARM
   RAM: 4 GB
   SSD: 250 GB NVME M2 Gen 4 (but the device recognize only Gen 2)   
   OS: Ubuntu 22.04.4 LTS Jammy   
   Python: 3.12.4

Times of the execution using _Shapefile_ input:

_Models_

| Model      | Time         |
|------------|--------------|
| ArcGIS Pro | 00:03:00.100 |
| QGIS       | 00:00:52.920 |

| Script                                                 | PC Desktop 1 | PC Desktop 2 | Orange Pi 3B 
|--------------------------------------------------------|--------------|--------------|--------------
| [process_arcgis_01.py](process_arcgis_01.py)           | 00:02:50.190 | -            | -            
| [process_arcgis_02.py](process_arcgis_02.py)           | 00:00:10.074 | -            | -            
| [process_geopandas.py](process_geopandas.py)           | 00:00:32.917 | 00:00:27.073 | 00:02:23.811 
| [process_dask-geopandas.py](process_dask-geopandas.py) | 00:00:34.189 | 00:00:30.036 | -            
| [process_duckdb_01.py](process_duckdb_01.py)           | 00:00:15.906 | 00:00:24.133 | 00:01:29.694 
| [process_pyqgis.py](process_pyqgis.py)                 | 00:00:58.562 | -            | -            

Times of the execution using _GeoParquet_ input:

| Script                                       | PC Desktop 1 | PC Desktop 2 | Orange Pi 3B |
|----------------------------------------------|--------------|--------------|--------------|
| [process_geopandas.py](process_geopandas.py) | 00:00:27.419 | 00:00:21.812 | 00:01:34.016 |
| [process_duckdb_02.py](process_duckdb_02.py) | 00:00:06.078 | 00:00:04.214 | 00:00:26.057 |


### Sources

* MapBiomas Alerta: https://plataforma.alerta.mapbiomas.org/mapa

* SICAR: https://consultapublica.car.gov.br/publico/estados/downloads

* GeoPandas: https://geopandas.org/en/stable

* Dask-GeoPandas: https://dask-geopandas.readthedocs.io/en/stable/index.html

* Duckdb Spatial Extension: https://duckdb.org/docs/extensions/spatial

* ESRI: Write geoprocessing output to
memory: https://pro.arcgis.com/en/pro-app/latest/help/analysis/geoprocessing/basics/the-in-memory-workspace.htm

* ESRI: Comparison of classic overlay tools to pairwise overlay
tools: https://pro.arcgis.com/en/pro-app/latest/tool-reference/analysis/comparison-of-classic-overlay-tools-to-pairwise-overlay-tools.htm
https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data?resource=download&select=yellow_tripdata_2015-01.csv