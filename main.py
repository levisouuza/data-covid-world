
# __init__.py
# -*- coding: UTF-8 -*-

from datetime import datetime, timedelta
from database import load_db
import pandas as pd
import transform
import schedule
import requests
import extract
import del_arq
import time
import os
import re


dir_inicial = '/home/levis/covidWorld'
url = 'http://localhost:15432/'

format_dif = list()

dt_ini = datetime.now().date() - timedelta(days=1)
dt = dt_ini.strftime('%m-%d-%Y')

carga_inicial = 'N'

def world_covid(directory, url):

    try:
        #criando diretório para persistir os arquivos csv.
        if not os.path.exists(directory):
            os.makedirs(directory)

        #excluindo arquivos caso existam.
        for dates in extract.gera_datas(dt):
            if os.path.exists(os.path.join(directory,f'{dates}.csv')):
                os.remove(os.path.join(directory,f'{dates}.csv'))

        time.sleep(1)

        #método responsável por criar requests do repositório John Hopkins
        extract.extract_git(directory, dt)

        time.sleep(2)

        #verificando arquivos que estão em formatos diferentes e padronizando-os para um único formato.
        if os.path.exists(directory):
            for arq in os.listdir(directory):

                # Condição que verifica o formato dos arquivo e direciona para sua respectiva estrutura de dados.
                if re.search('csv', arq):

                    if arq[:2] == '03' and arq[3:5] >= '22' and arq[6:10] == '2020':
                        format_dif.append(arq)
                    elif arq[:2] >= '04' and arq[6:10] >= '2020':
                        format_dif.append(arq)

            #método para transformação das features
            transform.trans_arquivos(directory, format_dif, directory)

        time.sleep(1)

        #método que irá criar a data das ocorrências dos datasets
        transform.feature_date(dir_inicial)

        time.sleep(2)

        #criação dataset full

        if carga_inicial == 'S':
            transform.load_dataset(dir_inicial, carga_inicial)

        elif carga_inicial == 'N':
            transform.load_dataset(dir_inicial, carga_inicial, extract.gera_datas(dt))

        time.sleep(2)

        del_arq.del_arq(dir_inicial)

        #try:

        #    requests.get(url).status_code

            #try:
        load_db.load_database('localhost', 'postgres', 'postgres', '1123581321', carga_inicial, dir_inicial, extract.gera_datas(dt))
            #except:
                #print('Erro de inclusão no banco.')

        #except:
        #    print('Falha na conexão.')

    except KeyboardInterrupt:
        print('Execução cancelada pelo usuário.')


world_covid(dir_inicial, url)
