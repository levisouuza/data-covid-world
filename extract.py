# __init__.py
# -*- coding: UTF-8 -*-
from datetime import datetime, timedelta
import pandas as pd
import requests
import time
import io
import os

def gera_datas(date_ini):
    '''
    :param date_ini: Data inicial que começará a lista.
    :return: lista com datas baseado na data inicial até a data atual - 1.
    '''
    dt_inicial = datetime.strptime(date_ini, '%m-%d-%Y').date()
    dates = list()

    for incr in range(0, (datetime.today().date() - dt_inicial).days, 1):
        dt_ref = dt_inicial + timedelta(days=incr)
        # print(incr, dt_ref.strftime('%m-%d-%Y'))
        dates.append(dt_ref.strftime('%m-%d-%Y'))

    return dates

def extract_git(directory,dt_inicial):
    '''
    :param directory: diretório que será persistidos os arquivos.
    :param dt_inicial: data inicial que criará uma lista de datas, onde cada data será o parâmetro para busca de url.
    :return: arquivos persistidos na pasta desejada
    '''

    if not os.path.exists(directory):
        os.makedirs(directory)

    for dt in gera_datas(dt_inicial):


        url = f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{dt}.csv'

        time.sleep(0.5)
        req = requests.get(url).content

        data = pd.read_csv(io.StringIO(req.decode('utf-8')))

        dataset = pd.DataFrame(data= data, columns=data.columns)

        #será excluído o csv para não haver duplicidade
        if os.path.exists(os.path.join(directory,f"{dt}.csv")):
            os.remove(os.path.join(directory,f"{dt}.csv"))

        dataset.to_csv(os.path.join(directory,f"{dt}.csv"), index=False)
