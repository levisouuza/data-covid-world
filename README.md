# Data Covid World
#### Extração, transformação e carga dos dados da covid19 mundiais.

[DataCovidWorld](https://github.com/levisouuza/data-covid-world/blob/master/main.py) realiza a busca dos arquivos do repositório da [Johns Hopkins University](https://github.com/CSSEGISandData/COVID-19) da Covid-19. Neste repositório contém dados históricos dos países vítimas do vírus. 

#### Data Pipeline
![Data Pipeline](https://github.com/levisouuza/data-covid-world/blob/master/images/fluxo.png)

1. A fonte de dados é o repositório do github da Johns Hopkins University. 

2. O tratamento de dados e a feature engineering foi realizado via python com a lib pandas. 

3. A carga foi realizada utilizando o banco de dados PostgreSql containerizado no Docker. 

#### 
![PostgreSql Docker](https://github.com/levisouuza/data-covid-world/blob/master/images/docker_ps.png) 

![PostgreSql Pgadmin](https://github.com/levisouuza/data-covid-world/blob/master/images/DataCovidWorld%20-%20PostgreSql.png) 
