"""
Script using pyqgis

Requires: A GIS installed in C:\OSGeo4W folder
For execution on Windows, run the batch file run_script_qgis.bat
Tested only Windows 10
"""

import os
import argparse
from pathlib import Path
import time

from qgis._core import QgsCoordinateReferenceSystem
from qgis.core import (QgsApplication, QgsProcessing)
from utils import info, info_finished

qgs = QgsApplication([], False)
qgs.initQgis()

import processing
from processing.core.Processing import Processing

Processing.initialize()


def run(car_file_path: str = '', mp_file_path: str = '',
        path_results: str = ''):
    try:
        start = time.perf_counter()

        info('Filtering CAR only contains "Analise"...')
        alg_params = {
            'EXPRESSION': '"des_condic" like \'%analise%\'',
            'INPUT': car_file_path,
            'OUTPUT': 'memory:'
        }
        filtered = processing.run('native:extractbyexpression',
                                  alg_params,
                                  is_child_algorithm=False)['OUTPUT']
        info('Intersection layers...')
        alg_params = {
            'GRID_SIZE': None,
            'INPUT': mp_file_path,
            'INPUT_FIELDS': [''],
            'OVERLAY': filtered,
            'OVERLAY_FIELDS': [''],
            'OVERLAY_FIELDS_PREFIX': '',
            'OUTPUT': 'memory:'
        }
        intersection = processing.run('native:intersection', alg_params,
                                      is_child_algorithm=False)['OUTPUT']

        info('Dissolving...')
        alg_params = {
            'FIELD': ['cod_imovel'],
            'INPUT': intersection,
            'SEPARATE_DISJOINT': False,
            'OUTPUT': 'memory:'
        }
        dissolve = processing.run('native:dissolve', alg_params,
                                  is_child_algorithm=False)['OUTPUT']

        info('Retain fields...')
        alg_params = {
            'FIELDS': ['cod_imovel'],
            'INPUT': dissolve,
            'OUTPUT': 'memory:'
        }
        retained = processing.run('native:retainfields', alg_params,
                                  is_child_algorithm=False)['OUTPUT']

        info('Transforming to UTM and calculating area...')
        alg_params = {
            'CONVERT_CURVED_GEOMETRIES': False,
            'INPUT': retained,
            'OPERATION': '',
            'TARGET_CRS': QgsCoordinateReferenceSystem('EPSG:31982'),
            'OUTPUT': 'memory:Reprojected'
        }
        reprojected = processing.run('native:reprojectlayer',
                                     alg_params,
                                     is_child_algorithm=False)['OUTPUT']

        alg_params = {
            'FIELD_LENGTH': 10,
            'FIELD_NAME': 'area_ha',
            'FIELD_PRECISION': 3,
            'FIELD_TYPE': 0,  # Decimal (double)
            'FORMULA': '$area/10000',
            'INPUT': reprojected,
            'OUTPUT': 'memory:'
        }
        calculated = processing.run(
            'native:fieldcalculator',
            alg_params,
            is_child_algorithm=False)['OUTPUT']

        info('Saving results...')
        file_results_path = Path(path_results, 'results_pyqgis.gpkg').__str__()

        alg_params = {
            'ACTION_ON_EXISTING_FILE': 0,
            'DATASOURCE_OPTIONS': '',
            'INPUT': calculated,
            'LAYER_NAME': 'results_pyqgis',
            'LAYER_OPTIONS': '',
            'OUTPUT': file_results_path,
        }
        processing.run('native:savefeatures', alg_params,
                       is_child_algorithm=True)

        qgs.exitQgis()
        info_finished(start_time=start)
    except Exception as e:
        info(f'Erro ao executar: {e.__str__()}')
    finally:
        qgs.exitQgis()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-car', type=str, required=True, dest='car',
        help='File path (Shapefile with extension .shp) from CAR'
    )
    parser.add_argument(
        '-mp', type=str, required=True, dest='mp',
        help='File path (Shapefile with extension .shp) from MapBiomas Alerta'
    )

    parser.add_argument(
        '-r', type=str, required=True, dest='r',
        help='Path to save the results'
    )

    args = parser.parse_args()

    car: str = args.car
    mp: str = args.mp
    path_results: str = args.r

    if not Path(car).exists():
        print(f'File {car} not found.')
        return
    if not Path(mp).exists():
        print(f'File {mp} not found.')
        return
    if not Path(path_results).exists():
        print(f'Path {path_results} not found.')
        return

    run(car_file_path=car, mp_file_path=mp, path_results=path_results)


if __name__ == '__main__':
    main()
