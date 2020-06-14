# __init__.py
# -*- coding: UTF-8 -*-
import pandas as pd
import os
import re

def trans_arquivos(dir_inicial, arquivos,dir_final):
    '''
    Função que transformará os datasets do covid-19 que estão com formatos diferentes para um formato padrão.

    :param dir_inicial: diretório de origem dos arquivos que serão transformados.
    :param arquivos:  lista que terá todos arquivos que serão transformados.
    :param dir_final:  diretório destino dos arquivos que serão transformados
    '''
    for i in arquivos:
        file = os.path.join(dir_inicial, i)
        arq = pd.read_csv(file)
        dataset = pd.DataFrame(data=arq, columns=arq.columns)

        dataset.rename(columns={'Lat': 'Latitude', 'Long_': 'Longitude', 'Country_Region': 'Country/Region',
                            'Province_State': 'Province/State', 'Last_Update': 'Last Update'}, inplace=True)

        os.remove(file)

        dataset.to_csv(file, index=False)


def feature_date(directory):
    '''
    Função que irá criar a data das ocorrências dos datasets

    :param directory: diretório em que será realizado a busca dos arquivos
    '''
    for arc in os.listdir(directory):
        file = os.path.join(directory, arc)
        arq = pd.read_csv(file)
        dataset = pd.DataFrame(data=arq, columns=arq.columns)

        dataset['Dt_registro'] = arc[:10]  # creating feature data registro = datas das notificações diárias

        new_file = dataset[
            ['Province/State', 'Country/Region', 'Last Update', 'Confirmed', 'Deaths', 'Recovered', 'Dt_registro']]

        new_file.to_csv(file, index=False)


def group_dataset(dir_inicial):

    '''
    Função agrupar todos os datasets em um dataset master

    :param direct: diretório em que será realizado a busca dos arquivos.
    :return master: dataframe agrupado
    '''

    archives = list()
    dataframes = list()

    for csv in os.listdir(dir_inicial):
        if not re.search('full', dir_inicial):
            directory = os.path.join(dir_inicial, csv)

            archives.append(directory)

    for dir in archives:
        df = pd.read_csv(dir)

        dataframes.append(df)

    master = pd.concat(dataframes, ignore_index=True)

    master.sort_values(['Dt_registro'], ascending=False, inplace=True)

    return master


def load_dataset(dir_inicial, carga_inicial, dates_incremental=None):
    '''
    Resposável por criar arquivo .csv em que será persistido os dados do DataFrame. As cargas podem ser integrais ou incrementais.

    :param direct: diretório em que será realizado a busca dos arquivos.
    :param carga_inicial: tipo de carga do arquivo. S é considerada a carga inicial e N, a carga incremental.
    :param dt: datas dos registro que estão sendo atualizados e/ou incluídos.
    '''

    if carga_inicial in 'S':
        #Instanciando o dataframe e criando um arquivo .csv.

        if os.path.exists(os.path.join(dir_inicial, 'covid_brasil_full.csv')):
            os.remove(os.path.join(dir_inicial, 'covid_brasil_full.csv'))

        master = group_dataset(dir_inicial)

        master.to_csv(os.path.join(dir_inicial,'covid_brasil_full.csv'), index=False)


    elif carga_inicial in 'N':

        update_dates = dates_incremental

        #vericação da existência do arquivo.
        if os.path.exists(os.path.join(dir_inicial, 'covid_brasil_full.csv')):
            full = pd.read_csv(os.path.join(dir_inicial, 'covid_brasil_full.csv'))

            #filtrando as informações que serão atualizadas e realizando a exclusão..
            for dates in update_dates:
                remove = full[full['Dt_registro'] == dates]
                full.drop(remove.index, inplace=True)

            #concatenando os dados do dataset antigo com dataset gerado no momento.
            dataset_update = pd.concat([full, group_dataset(dir_inicial)], ignore_index=False)
            dataset_update.sort_values(['Dt_registro'], ascending=False, inplace=True)

            #excluíndo arquivo full anterior para criando do arquivo atualizado.
            os.remove(os.path.join(dir_inicial, 'covid_brasil_full.csv'))

            dataset_update.to_csv(os.path.join(dir_inicial,'covid_brasil_full.csv'), index=False)
