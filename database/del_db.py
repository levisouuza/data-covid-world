# __init__.py
# -*- coding: UTF-8 -*-

from datetime import datetime, timedelta
import psycopg2 as pg

def delete_db(host, db, user, password, dates_incremental):

    connection = pg.connect(
            host= host,
            database= db,
            user= user,
            password= password
    )

    cursor = connection.cursor()

    try:
        for dates in dates_incremental:
            cursor.execute(f"DELETE FROM public.data_covid_world WHERE dt_registro = '{dates}'")
            connection.commit()

    except:
        print('Não foi possível exlcluir os registro do banco de dados.')
