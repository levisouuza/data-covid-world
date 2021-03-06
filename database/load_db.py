# __init__.py
# -*- coding: UTF-8 -*-

from datetime import datetime, timedelta
from math import trunc
import psycopg2 as pg
import pandas as pd
from database import insert_db
import requests
from database import del_db
import time
import os


def load_database(host, db, user, password, carga_inicial, dir_inicial, dates_incremental=None):
    '''
    Função que irá inserir registro no banco de dados.
    :param host: host do banco de dados.
    :param db: banco de dados
    :param user: user do banco de dados:
    :param password: senha do banco de dados.
    :param carga_inicial: tipo de carga do arquivo. S é considerada a carga inicial e N, a carga incremental.
    :param dir_inicial: diretório de origem dos arquivos que serão inseridos no banco de dados.
    '''
    connection = pg.connect(
            host= host,
            database= db,
            user= user,
            password= password
    )

    dataset = pd.read_csv(os.path.join(dir_inicial, 'covid_brasil_full.csv'))

    cursor = connection.cursor()

    #situacao para cargas inicial
    if carga_inicial == 'S':

        cursor.execute("TRUNCATE TABLE public.data_covid_world")

        connection.commit()

        insert_db.insert_db(host, db, user, password, dataset)

    #situacao para carga incremental
    elif carga_inicial == 'N':

        del_db.delete_db(host, db, user, password, dates_incremental)

        for dates in dates_incremental:
            dataset_new = dataset.query(f"Dt_registro == '{dates}'")

            insert_db.insert_db(host, db, user, password, dataset_new)
