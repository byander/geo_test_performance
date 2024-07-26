import argparse
import geopandas as gpd
from pathlib import Path
import time
from utils import info, info_finished


def run(car_file_path: str = '', mp_file_path: str = '',
        path_results: str = ''):
    start = time.perf_counter()

    info('Loading layers...')
    cols_car = ['cod_imovel', 'des_condic', 'geometry']
    cols_mp = ['geometry']

    check_extension = Path(car_file_path).suffix.lower()
    if check_extension == '.shp':
        gdf_car = gpd.read_file(car_file_path, columns=cols_car)
        gdf_mp = gpd.read_file(mp_file_path, columns=cols_mp)

    elif check_extension == '.parquet':
        gdf_car = gpd.read_parquet(car_file_path, columns=cols_car)
        gdf_mp = gpd.read_parquet(mp_file_path, columns=cols_mp)

        gdf_car.set_crs(epsg=4674, inplace=True, allow_override=True)
        gdf_mp.set_crs(epsg=4674, inplace=True, allow_override=True)

    # I hoped this worked more fast if I used the where clause to read only
    # the rows of the interest, but not
    # where_clause = "des_condic like '%analise%'"
    # gdf_car = gpd.read_file(car_file_path, columns=cols_car, where=where_clause)

    info('Filtering CAR only contains "Analise"...')
    # Select CAR only contanis the world 'analise'
    gdf_car = gdf_car[gdf_car['des_condic'].str.contains('analise')]

    info('Intersection layers...')
    # gdf_intersect = gpd.sjoin(gdf_mp, gdf_car, how='inner',
    #                           predicate='intersects')

    # TODO: The methods below has the same time for execution
    gdf_intersect = gpd.overlay(gdf_car, gdf_mp, how='intersection')
    # gdf_intersect = gdf_car.overlay(gdf_mp, how='intersection')

    info('Dissolving...')
    gdf_dissolve = gdf_intersect.dissolve(by='cod_imovel')

    del gdf_car, gdf_mp, gdf_intersect

    info('Transforming to UTM and calculating area...')
    gdf_dissolve = gdf_dissolve.to_crs(epsg=31982).reset_index()
    gdf_dissolve['area_ha'] = gdf_dissolve.area / 10000
    gdf_dissolve = gdf_dissolve[['cod_imovel', 'area_ha', 'geometry']]

    info('Saving results...')
    file_results = Path(path_results, 'results_geopandas.gpkg').__str__()
    gdf_dissolve.to_file(file_results, driver='GPKG',
                         layer='res_geopandas')

    info_finished(start_time=start)


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
