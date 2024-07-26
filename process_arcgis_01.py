"""
Script using arcpy

Requires: A valid license for ArcGIS Pro
"""

import argparse
from pathlib import Path
import time
from utils import info, info_finished

try:
    import arcpy
except Exception as e:
    print(f'Erro ao importar arcpy: {e.__str__()}')


def run(car_file_path: str = '',
        mp_file_path: str = '',
        path_results: str = ''):
    start = time.perf_counter()

    arcpy.env.overwriteOutput = True

    temp_gdb = Path(path_results, 'temp.gdb').__str__()
    if Path(temp_gdb).exists():
        arcpy.Delete_management(temp_gdb)

    arcpy.management.CreateFileGDB(path_results, 'temp.gdb')
    arcpy.env.workspace = temp_gdb

    info('Filtering CAR only contains "Analise"...')
    car_selected = 'car_selected'
    arcpy.analysis.Select(in_features=car_file_path,
                          out_feature_class=car_selected,
                          where_clause="\"des_condic\" like '%analise%'")

    info('Intersection layers...')
    intersect = 'car_x_mp'
    arcpy.analysis.Intersect(
        in_features=[[car_selected, ""], [mp_file_path, ""]],
        out_feature_class=intersect)

    info('Dissolving...')
    dissolve = 'car_x_mp_dissolve'
    arcpy.management.Dissolve(in_features=intersect,
                              out_feature_class=dissolve,
                              dissolve_field=["cod_imovel"])

    info('Transforming to UTM...')
    dissolve_utm = 'dissolve_utm'
    out_crs = arcpy.SpatialReference(31982)
    arcpy.Project_management(dissolve, dissolve_utm, out_crs)

    info('Calculating area...')
    arcpy.management.CalculateGeometryAttributes(
        in_features=dissolve_utm,
        geometry_property=[["area_ha", "AREA"]], area_unit="HECTARES")

    info('Saving results...')
    file_results = Path(path_results, 'results_arcpy_01.gpkg').__str__()
    arcpy.management.CreateSQLiteDatabase(
        out_database_name=file_results,
        spatial_type="GEOPACKAGE_1.3")

    cod_imovel_fm = arcpy.FieldMap()
    area_ha_fm = arcpy.FieldMap()
    fms = arcpy.FieldMappings()

    cod_imovel_fm.addInputField(dissolve_utm, 'cod_imovel')
    area_ha_fm.addInputField(dissolve_utm, 'area_ha')
    fms.addFieldMap(cod_imovel_fm)
    fms.addFieldMap(area_ha_fm)

    layer_results = Path(file_results, 'results_arcpy_01').__str__()
    arcpy.conversion.ExportFeatures(in_features=dissolve_utm,
                                    field_mapping=fms,
                                    out_features=layer_results)

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
