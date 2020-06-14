import os
import re

dir = '/home/levis/covidWorld'

def del_arq(dir_inicial):

    for csv in os.listdir(dir_inicial):
        directory = os.path.join(dir_inicial, csv)

        if not re.search('full', directory):
            os.remove(directory)
