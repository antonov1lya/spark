# Лабораторная работа №2
Выполнил: Антонов Илья, 24 МАГ ИАД
## Датасет
Используется датасет `nyc_yellow_taxi.csv`. 
Он состоит из первых 200.000 строчек датасет [NYC Yellow Taxi Trip Data](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data) за 2015 год. 
## Запуск
- Поднимите докер контейнеры через команды:
```
docker-compose -f docker-compose.yml up -d
```
или
```
docker-compose -f docker-compose3.yml up -d
```
в зависимости от конфигурации.
- Загрузите в hdfs датасет и ограничьте используемую память:
```
docker cp nyc_yellow_taxi.csv namenode:/
docker exec -it namenode /bin/bash
hdfs dfs -mkdir /data
hdfs dfs -D dfs.block.size=32M -put nyc_yellow_taxi.csv /data/
hdfs dfsadmin -setSpaceQuota 1g /data
```
- Запустите Spark Application:
```
docker cp application.py spark-master:/
docker exec -it spark-master /bin/bash
apk add --update make automake gcc g++ python-dev
apk add linux-headers
pip install numpy psutil
/spark/bin/spark-submit application.py
```
- Выключите контейнеры:
```
docker-compose -f docker-compose.yml down
```
или
```
docker-compose -f docker-compose3.yml down
```
## Результаты эксперментов:
| Конфигурация | Время (сек) | Потребление памяти (МБ) |
|-------------|-------------|-------------|
| 1 DataNode, Spark    | 46.18    | 36.00    |
| 1 DataNode, Spark Opt    | 47.22    | 36.00    |
| 3 DataNode, Spark    | 48.54    | 36.00    |
| 3 DataNode, Spark Opt    | 46.14    | 36.00    |
