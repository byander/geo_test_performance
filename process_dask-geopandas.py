"""
Script to process GeoParquet input using Dask-Geopandas

"""

import argparse
import geopandas as gpd
import dask_geopandas as dgpd
from pathlib import Path
import time
from datetime import datetime, timedelta
from utils import info


n_parts = 4  # Number of partitions


def run(car_file_path: str = '', mp: str = '', path_results: str = ''):
    start = time.perf_counter()

    info('Loading layers...')
    cols_car = ['cod_imovel', 'des_condic', 'geometry']
    gdf_car = gpd.read_file(car_file_path, columns=cols_car)

    cols_mp = ['geometry']
    gdf_mp = gpd.read_file(mp, columns=cols_mp)

    info('Filtering CAR only contains "Analise"...')
    # Select CAR only contanis the world 'analise'
    gdf_car = gdf_car[gdf_car['des_condic'].str.contains('analise')]

    info('Intersection layers...')
    gdf_intersect = gpd.overlay(gdf_car, gdf_mp, how='intersection')
    del gdf_car, gdf_mp

    info('Dissolving...')
    dgdf_intersect = dgpd.from_geopandas(gdf_intersect, npartitions=n_parts)
    gdf_dissolve = dgdf_intersect.dissolve(by='cod_imovel').compute()

    del dgdf_intersect, gdf_intersect

    info('Transforming to UTM and calculating area...')
    gdf_dissolve = gdf_dissolve.to_crs(epsg=31982).reset_index()
    gdf_dissolve['area_ha'] = gdf_dissolve.area / 10000
    gdf_dissolve = gdf_dissolve[['cod_imovel', 'area_ha', 'geometry']]

    info('Saving results...')
    file_results = Path(path_results,
                        'results_dask-geopandas.gpkg').__str__()
    gdf_dissolve.to_file(file_results, driver='GPKG',
                         layer='res_dask-geopandas')

    finished = time.perf_counter()
    total_time = timedelta(seconds=finished - start)
    total_time_format = (datetime.min + total_time).time().strftime(
        "%H:%M:%S.%f")[:-3]

    info(f'Finished in {total_time_format}')


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

    run(car_file_path=car, mp=mp, path_results=path_results)


if __name__ == '__main__':
    main()
