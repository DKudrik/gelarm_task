import datetime as dt
import os
import re
from os import getcwd, listdir
from os.path import isfile, join

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection

load_dotenv()

r17 = None

CREDS = {
    'dbname': os.environ.get('POSTGRES_DB'),
    'user': os.environ.get('POSTGRES_USER'),
    'password': os.environ.get('POSTGRES_PASSWORD'),
    'host': os.environ.get('HOST'),
    'port': os.environ.get('PORT'),
}


def set_connection() -> _connection:
    with psycopg2.connect(**CREDS) as pg_conn:
        return pg_conn


def is_name_contains_etalon_form(file: str) -> bool:
    """
    Проверка наличия в названии файла фразы 'форма эталон'. Перед проверкой название предварительно приводится
    к нижнему регистру.
    :param file: путь к файлу
    :return: bool
    """
    if re.match('^форма эталон.*$', file.lower()):
        return True
    return False


def is_date_in_name(file: str) -> bool:
    """
    Проверка наличия в названии файла даты в формате ДД.ММ.ГГГГ
    :param file: путь к файлу
    :return: bool
    """
    if re.search('.*\d{2}.\d{2}.\d{4}.*', file):
        return True
    return False


def is_date_in_r1(file: str) -> bool:
    """
    Проверка наличия даты в первой строке (вместо R1)
    :param file: путь к файлу
    :return: bool
    """
    df = pd.read_excel(file)
    global r17
    r17 = df.columns[17]
    if isinstance(r17, dt.datetime):
        return True
    return False


def count_prev_datasets(file_name: str) -> int:
    """
    Подсчет количества предыдущих датасетов
    :param file_name:
    :return:
    """
    df = pd.read_excel(file_name)
    prev_datasets = df.iloc[1][10:]
    prev_datasets_num = len(re.findall(r'\b\d{4} год\b', str(prev_datasets.values)))
    return prev_datasets_num


def find_project_id(idx, df):
    row = df.iloc[idx -1]
    prj_id = None
    if isinstance(row['C'], dt.datetime) and row['C'].date() == r17.date() and len(re.findall('\.', row['A'])) == 1:
        prj_id = re.search('\d', row['B'])[0]
    else:
        data = find_project_id(idx -1, df)
        if data:
            return data
    return prj_id


def if_in_db(prj_name: str) -> bool:
    with set_connection().cursor() as curs:
        curs.execute('SELECT * FROM federal_projects WHERE name=%s', (prj_name, ))
        data = curs.fetchall()
        if data:
            return True
        return False


def add_to_db(prj_name):
    ...


def process_data(file_name: str):
    df = pd.read_excel(file_name)
    col_list = [chr(i + 65) for i in range(len(df.columns))]
    df.columns = col_list
    r17_date = r17.date()
    for idx, row in list(df.iterrows()):
        if isinstance(row['C'], dt.datetime) and row['C'].date() == r17_date:
            if len(re.findall('\.', row['A'])) == 1:
                federal_prj_id = re.search('\d', row['B'])[0]
                prj_name = row['B']
                if not if_in_db(prj_name):
                    add_to_db(prj_name)
            elif len(re.findall('\.', row['A'])) == 2:
                federal_prj_id = find_project_id(idx, df)
            print(idx, federal_prj_id)


def main():
    file_names = [
        f for f in listdir(getcwd())
        if isfile(join(getcwd(), f)) and is_name_contains_etalon_form(f) and is_date_in_name(f) and is_date_in_r1(f)
    ]

    for name in file_names:
        prev_datasets_num = count_prev_datasets(name)
        process_data(name)


if __name__ == '__main__':
    main()
