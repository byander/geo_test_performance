import argparse
import duckdb as dkb
from duckdb import DuckDBPyConnection
from pathlib import Path
import time
from datetime import datetime, timedelta
from utils import info


class ETL:

    @staticmethod
    def _create_db_memory() -> DuckDBPyConnection:
        con = dkb.connect()
        con.install_extension("spatial")
        con.load_extension("spatial")

        return con

    @staticmethod
    def _create_tables(con: DuckDBPyConnection = None,
                       file_path: str = '',
                       table_name: str = '',
                       cols: list = None) -> None:
        cols_str = ', '.join(cols) if cols else '*'
        sql = """
        CREATE TEMPORARY TABLE IF NOT EXISTS {0} AS 
        SELECT 
            {1} 
        FROM ST_Read('{2}')
        """.format(table_name, cols_str, file_path)
        con.execute(sql.format(table_name))

    def run(self, car_file_path: str = '', mp: str = '',
            path_results: str = ''):
        start = time.perf_counter()

        con = self._create_db_memory()

        car_table_name = 'CAR'
        cols_car = ['cod_imovel', 'des_condic', 'geom']
        map_biomas_table_name = 'MP'
        cols_mp = ['geom']

        info('Loading layers...')
        self._create_tables(con=con, file_path=car_file_path,
                            table_name=car_table_name,
                            cols=cols_car)
        self._create_tables(con=con, file_path=mp,
                            table_name=map_biomas_table_name,
                            cols=cols_mp)

        info('Filtering CAR only contains "Analise"...')
        sql = """
        CREATE OR REPLACE TEMPORARY TABLE CAR AS 
        SELECT 
            c.cod_imovel, c.geom FROM {0} c 
        WHERE 
            c.des_condic LIKE '%analise%';
        """.format(car_table_name)
        con.execute(sql)

        info('Intersection layers...')
        sql = """
        CREATE OR REPLACE TEMPORARY TABLE CAR AS 
        SELECT
            c.cod_imovel,
            ST_Intersection(c.geom, m.geom) AS geom
        FROM
            {0} c
        JOIN {1} m ON ST_Intersects(c.geom, m.geom);
        """.format(car_table_name, map_biomas_table_name)
        con.execute(sql)

        info('Dissolving...')
        sql = """
        CREATE OR REPLACE TEMPORARY TABLE CAR AS 
        SELECT
            c.cod_imovel,
            ST_Union_Agg(c.geom) AS geom
        FROM
            CAR c
        GROUP BY
            c.cod_imovel;
        """
        con.execute(sql)

        info('Transforming to UTM...')
        sql = """
        CREATE OR REPLACE TEMPORARY TABLE CAR AS 
        SELECT 
            * EXCLUDE geom,
            ST_Transform(geom, 'EPSG:4326', 'EPSG:31982', true) AS geom,
        FROM
            CAR;
        """
        con.execute(sql)

        info('Calculating area...')
        sql = """
        CREATE OR REPLACE TEMPORARY TABLE RESULTS AS
        SELECT
            cod_imovel,
            ST_Area(geom) / 10000 AS area_ha,
            geom
        FROM
            CAR;
        """
        con.execute(sql)

        info('Saving results...')
        file_results = Path(path_results, 'results_duckdb.gpkg').__str__()
        sql = """
        COPY RESULTS TO '{0}'
        WITH (FORMAT GDAL, DRIVER 'GPKG', LAYER_NAME 'resultados_duckdb', SRS 'EPSG:31982')
        """.format(file_results)
        con.execute(sql)

        finished = time.perf_counter()
        total_time = timedelta(seconds=finished - start)
        total_time_format = (datetime.min + total_time).time().strftime(
            "%H:%M:%S.%f")[:-3]

        con.close()
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

    etl = ETL()
    etl.run(car_file_path=car, mp=mp, path_results=path_results)


if __name__ == '__main__':
    main()
