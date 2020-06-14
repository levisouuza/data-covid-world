# __init__.py
# -*- coding: UTF-8 -*-

from datetime import datetime
from math import trunc
import psycopg2 as pg
import time


def insert_db(host, db, user, password, dataset):

    connection = pg.connect(
            host= host,
            database= db,
            user= user,
            password= password
    )

    cursor = connection.cursor()

    inicio_insercao = time.time()

    for row in range(len(dataset)):

        province = dataset.iloc[row][0]
        country = dataset.iloc[row][1]
        last_update = dataset.iloc[row][2]
        confirmed = int(dataset.iloc[row][3])
        deaths = int(dataset.iloc[row][4])
        recovered = int(dataset.iloc[row][5])
        dt_registro = dataset.iloc[row][6]

        cursor.execute("insert into public.data_covid_world(province_state,country_region, last_update,confirmed, deaths, recovered, dt_registro)values(%s,%s,%s,%s,%s,%s,%s)",
                        (province, country, last_update, confirmed, deaths, recovered, dt_registro))

        connection.commit()

        time.sleep(0.05)

        if row == 0:
            print(f'{datetime.now()} - Iniciando inclusão dos dados. Serão incluídos {len(dataset)} registros.')

        elif row != 0 and row % 100 ==0:
            print(f'{datetime.now()} - Já fora inseridos {row} registros no seu banco de dados.')

    print(f"{datetime.now()} - A inclusão dos dados foi finalizada. Foram inseridos {len(dataset)} em {trunc(time.time() - inicio_insercao)} segundos.")
