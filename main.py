import datetime as dt
import os
import re
from os import getcwd, listdir
from os.path import isfile, join

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection

from utils import is_organization, is_project, is_equal_to_r1

load_dotenv()

r1 = None

CREDS = {
    'dbname': os.environ.get('POSTGRES_DB'),
    'user': os.environ.get('POSTGRES_USER'),
    'password': os.environ.get('POSTGRES_PASSWORD'),
    'host': os.environ.get('HOST'),
    'port': os.environ.get('PORT'),
}

TABLES = {
    'proj': 'federal_projects',
    'org': 'federal_organizations',
    'proj_del': 'federal_projects_delayed',
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
    Проверка наличия даты в R1
    :param file: путь к файлу
    :return: bool
    """
    df = pd.read_excel(file)
    global r1
    r1 = df.columns[17]
    if isinstance(r1, dt.datetime):
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
    if isinstance(row['C'], dt.datetime) and row['C'].date() == r1.date() and len(re.findall('\.', row['A'])) == 1:
        prj_id = re.search('\d', row['B'])[0]
    else:
        data = find_project_id(idx -1, df)
        if data:
            return data
    return prj_id


def is_in_db(table, name: str) -> bool:
    with set_connection().cursor() as curs:
        curs.execute(f'SELECT * FROM {TABLES[table]} WHERE name=%s', (name, ))
        data = curs.fetchall()
        if data:
            return True
        return False


def add_to_db(table, name: str):
    pg_conn = set_connection()
    with pg_conn.cursor() as curs:
        curs.execute(f'INSERT INTO {TABLES[table]} (name) VALUES (%s)', (name, ))
        pg_conn.commit()
        curs.execute('SELECT LASTVAL()')
        record_id = curs.fetchone()[0]
        return record_id


def find_org_id(org_name: str):
    pg_conn = set_connection()
    with pg_conn.cursor() as curs:
        curs.execute(f'SELECT id FROM {TABLES["org"]} WHERE name=%s', (org_name, ))
        org_id = curs.fetchone()[0]
        return org_id


def add_dataset_to_db(**kwargs):
    pg_conn = set_connection()
    with pg_conn.cursor() as curs:
        fields = ', '.join(kwargs.keys())
        curs.execute(f'INSERT INTO {TABLES["proj_del"]} ({fields}) VALUES ({", ".join(["%s"] * len(kwargs))})', list(kwargs.values()))
        pg_conn.commit()


def process_data(file_name: str, prev_datasets_num: int):
    df = pd.read_excel(file_name)
    col_list = [chr(i + 65) for i in range(len(df.columns))]
    df.columns = col_list
    total_row = df.loc[df['A'] == 'Итого'].index.tolist()[0]
    r1_date = r1.date()
    for idx, row in list(df.iterrows())[:total_row]:
        if is_equal_to_r1(row, r1_date):
            if is_project(row):
                prj_name = row['B']
                if not is_in_db('proj', prj_name):
                    federal_prj_id = add_to_db('proj', prj_name)
                federal_org_id = None
            elif is_organization(row):
                org_name = row['B']
                if not is_in_db('org', org_name):
                    add_to_db('org', org_name)
                federal_org_id = find_org_id(org_name)

            prj_date = row['C']

            # обработка датасета за текущий год
            year_no = int(df.iloc[1, 3][:4])
            year_plan = df.iloc[idx, 3]
            year_achieved_cnt = df.iloc[idx, 4]
            year_achieved_percent = df.iloc[idx, 5] * 100
            year_left_cnt = df.iloc[idx, 6]
            year_left_percent = df.iloc[idx, 7] * 100
            year_delayed_cnt = df.iloc[idx, 8]
            year_delayed_percent = df.iloc[idx, 9] * 100
            total_delayed_cnt = df.iloc[idx, -2]
            total_delayed_percent = df.iloc[idx, -1] * 100

            date = dt.datetime.strptime(re.search('\d{2}.\d{2}.\d{4}', file_name).group(0), '%d.%m.%Y')
            created_from = date - dt.timedelta(days=date.weekday())
            created_to = created_from + dt.timedelta(days=6)

            relevance_dttm = r1_date

            add_dataset_to_db(federal_prj_id=federal_prj_id, federal_org_id=federal_org_id, prj_date=prj_date,
                              year_no=year_no, year_plan=year_plan, year_achieved_cnt=year_achieved_cnt,
                              year_achieved_percent=year_achieved_percent, year_left_cnt=year_left_cnt,
                              year_left_percent=year_left_percent, year_delayed_cnt=year_delayed_cnt,
                              year_delayed_percent=year_delayed_percent, total_delayed_cnt=total_delayed_cnt,
                              total_delayed_percent=total_delayed_percent, created_from=created_from,
                              created_to=created_to, relevance_dttm=relevance_dttm)

            # обработка датасетов прошлых годов
            dataset_length = 5
            for i in range(prev_datasets_num):
                year_no = int(df.iloc[1, 10 + dataset_length*i][:4])
                year_plan = df.iloc[idx, 10 + dataset_length*i]
                year_achieved_cnt = df.iloc[idx, 11 + dataset_length*i]
                year_achieved_percent = df.iloc[idx, 12 + dataset_length*i] * 100
                year_delayed_cnt = df.iloc[idx, 13 + dataset_length*i]
                year_delayed_percent = df.iloc[idx, 14 + dataset_length*i] * 100
                total_delayed_cnt = df.iloc[idx, -2]
                total_delayed_percent = df.iloc[idx, -1] * 100

                date = dt.datetime.strptime(re.search('\d{2}.\d{2}.\d{4}', file_name).group(0), '%d.%m.%Y')
                created_from = date - dt.timedelta(days=date.weekday())
                created_to = created_from + dt.timedelta(days=6)

                relevance_dttm = r1_date

                add_dataset_to_db(federal_prj_id=federal_prj_id, federal_org_id=federal_org_id, prj_date=prj_date,
                                  year_no=year_no, year_plan=year_plan, year_achieved_cnt=year_achieved_cnt,
                                  year_achieved_percent=year_achieved_percent, year_delayed_cnt=year_delayed_cnt,
                                  year_delayed_percent=year_delayed_percent, total_delayed_cnt=total_delayed_cnt,
                                  total_delayed_percent=total_delayed_percent, created_from=created_from,
                                  created_to=created_to, relevance_dttm=relevance_dttm)

    # обработка строки "Итого"
    for idx, row in list(df.iterrows())[total_row:]:
        federal_prj_id = 1
        federal_org_id = None
        if is_equal_to_r1(row, r1_date):
            # обработка датасета за текущий год
            prj_date = row['C']
            year_no = int(df.iloc[1, 3][:4])
            year_plan = df.iloc[idx, 3]
            year_achieved_cnt = df.iloc[idx, 4]
            year_achieved_percent = df.iloc[idx, 5] * 100
            year_left_cnt = df.iloc[idx, 6]
            year_left_percent = df.iloc[idx, 7] * 100
            year_delayed_cnt = df.iloc[idx, 8]
            year_delayed_percent = df.iloc[idx, 9] * 100
            total_delayed_cnt = df.iloc[idx, -2]
            total_delayed_percent = df.iloc[idx, -1] * 100

            date = dt.datetime.strptime(re.search('\d{2}.\d{2}.\d{4}', file_name).group(0), '%d.%m.%Y')
            created_from = date - dt.timedelta(days=date.weekday())
            created_to = created_from + dt.timedelta(days=6)

            relevance_dttm = r1_date

            add_dataset_to_db(federal_prj_id=federal_prj_id, federal_org_id=federal_org_id, prj_date=prj_date,
                              year_no=year_no, year_plan=year_plan, year_achieved_cnt=year_achieved_cnt,
                              year_achieved_percent=year_achieved_percent, year_left_cnt=year_left_cnt,
                              year_left_percent=year_left_percent, year_delayed_cnt=year_delayed_cnt,
                              year_delayed_percent=year_delayed_percent, total_delayed_cnt=total_delayed_cnt,
                              total_delayed_percent=total_delayed_percent, created_from=created_from,
                              created_to=created_to, relevance_dttm=relevance_dttm)

            # обработка датасетов прошлых годов
            dataset_length = 5
            for i in range(prev_datasets_num):
                year_no = int(df.iloc[1, 10 + dataset_length*i][:4])
                year_plan = df.iloc[idx, 10 + dataset_length*i]
                year_achieved_cnt = df.iloc[idx, 11 + dataset_length*i]
                year_achieved_percent = df.iloc[idx, 12 + dataset_length*i] * 100
                year_delayed_cnt = df.iloc[idx, 13 + dataset_length*i]
                year_delayed_percent = df.iloc[idx, 14 + dataset_length*i] * 100
                total_delayed_cnt = df.iloc[idx, -2]
                total_delayed_percent = df.iloc[idx, -1] * 100

                date = dt.datetime.strptime(re.search('\d{2}.\d{2}.\d{4}', file_name).group(0), '%d.%m.%Y')
                created_from = date - dt.timedelta(days=date.weekday())
                created_to = created_from + dt.timedelta(days=6)

                relevance_dttm = r1_date

                add_dataset_to_db(federal_prj_id=federal_prj_id, federal_org_id=federal_org_id, prj_date=prj_date,
                                  year_no=year_no, year_plan=year_plan, year_achieved_cnt=year_achieved_cnt,
                                  year_achieved_percent=year_achieved_percent, year_delayed_cnt=year_delayed_cnt,
                                  year_delayed_percent=year_delayed_percent, total_delayed_cnt=total_delayed_cnt,
                                  total_delayed_percent=total_delayed_percent, created_from=created_from,
                                  created_to=created_to, relevance_dttm=relevance_dttm)


def main():
    file_names = [
        f for f in listdir(getcwd())
        if isfile(join(getcwd(), f)) and is_name_contains_etalon_form(f) and is_date_in_name(f) and is_date_in_r1(f)
    ]

    for name in file_names:
        prev_datasets_num = count_prev_datasets(name)
        process_data(name, prev_datasets_num)


if __name__ == '__main__':
    main()
