#!/usr/bin/env_python
# coding: utf-8

# ETL скрипт который преобразовывает таблицу с событиями,
# насышает её данными из второй таблицы и загружает в BQ с такой схемой
# -app_name (string)
# -app_version (string)
# -created_at (timestamp)
# -event_name (string)
# -user_id (int)
# -screen_name (string)
# -product_id (string)
# -city_catalog (int)
# -сompleted_purchases (int)
# -total_revenue (int)
# Уточнения:
# -product_id (список, пример “57364911” или “58935089,58996453,59452609”)
# -сompleted_purchases (количество завершенных покупок на данный момент времени, брать из таблицы orders_lite)
# -total_revenue (сумма дохода со всех завершенных покупок на данный момент времени, брать из таблицы orders_lite)
# Creation date: 2022-06-23
# Author: Shaposhnikov Sergey

import re
import logging
from google.cloud import bigquery
from google.oauth2 import service_account
from flatten_json import flatten
import pandas as pd


CREDENTIALS = service_account.Credentials.from_service_account_file(
    '../hopeful-ally-307310-11d59346797f.json')

PROJECT = 'hopeful-ally-307310'

EVENTS_TABLE = 'hopeful-ally-307310.test_de.events_data'
TARGET_TABLE = 'hopeful-ally-307310.test_de.output_shaposhnikov2'

PATH = '../orders_lite.csv'

def setup_logger(name):
    """ Setup logger
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        # Prevent logging from propagating to the root loggger
        logger.propagate = 0
        console = logging.StreamHandler()
        logger.addHandler(console)
        formatter = logging.Formatter('%(asctime)s \t%(levelname)s \t%(processName)s \t%(message)s')
        console.setFormatter
    return logger


def extract_events(table: str, project: str, credentials) -> pd.DataFrame:
    """This function extract data from BQ table

    Args:
        table (str): we need to extract
        project (str): Project name
        credentials: Credentials_service_acc

    Returns:
        pd.DataFrame: return extracted data as pd.DataFrame
    """

    sql = f"SELECT * FROM {table}"

    with bigquery.Client(project, credentials) as client:
        try:
            query = client.query(sql)
            logger.info("Клиент подключился к проекту - %s", query.project)
            _ = query.result().to_dataframe(create_bqstorage_client=True)
            logger.info("Датафрейм успешно выгружен. Строчек в нем - %s", len(_))
        except Exception as err:
            logger.info("Ошибка при подключение к базе %s", err)
    return _


def transform_flatten_json(df: pd.DataFrame, column='_publisher_parameters_') -> pd.DataFrame:
    """ This function returns DataFrame unwrapped column which
     is a flatten json

    Args:
        df (pd.DataFrame): source DataFrame
        column (str): colname we need to unwrap

    Returns:
        pd.DataFrame: transformed DataFrame
    """
    try:
        unwraped = pd.DataFrame([flatten(eval(_)) for _ in df[column].to_list()])
        _ = pd.concat([df, unwraped], axis=1).drop(columns=[column])
        logger.info("Столбец с JSON успешно развернут - %s", len(_))
    except Exception as err:
        logger.info("Ошибка при разворачивания json", err)
    return _


def create_dict(path: str) -> dict:
    """
    Creating dictionary  where keys - user_id
    and values is tuple  - (total_orders, total_sum)

    Args:
        path (str): path to file with data

    Returns:
        dict: created dict
    """
    try:
        orders_lite = pd.read_csv(path)
        logger.info("Файл загружен в DataFrame - %s", len(orders_lite))
        # Агреггация данных
        enrich = orders_lite.groupby('user_id').agg(Count=('user_id', 'count'), Sum=('sum', 'sum'))
        logger.info("Агрегация прошла успешно")
        _ = dict(zip(enrich.index, [*zip(enrich.Count, enrich.Sum)]))
        logger.info("Словарь успешно успешно создан, в нем - %s значений", len(_))
    except Exception as err:
        logger.info("Ошибка при создания user словаря", err)
    return _


def resulted_dframe(df: pd.DataFrame, user_dict: dict) -> pd.DataFrame:
    """
    This function servee to data saturation our
    sources dataframe by created user values.

    Args:
        df (pd.DataFrame): sources dataframe
        user_dict (dict): users ordes data

    Returns:
        pd.DataFrame: transformed 
    """
    try:
        df[['сompleted_purchases', 'total_revenue']] = (pd.DataFrame(df['user_id'].astype(int).map(user_dict) .to_list(), index=df.index))
        logger.info("Фрейм данных обогащен новыми данным")
    except Exception as err:
        logger.info("Какая-то ошибка на момента создания  user словаря", err)
    return df


def rename_final(df: pd.DataFrame):
    """
    Rename our resulted df, and extracting columns by needed order.
    PS:  Я нашел русскую букву в имени столбца, таким вот списковым включением
    на Python (может пригодится):
    [re.sub(r'[^A-Za-z_0-9]', '!', cn) for cn in final.columns]
    вроде там была c первая русская, поэтому при инсерте данных
    выкидывало ошибку. Я автоматически переименовываю ошибку,
    тут можно конечно написать функцию отправки ошибки, что в колонке присутсвуют
    русские буквы и слать имя колонки. 
    Чем-от вроде этого - _ = len([_ for _ in df_columns if re.findall(r'[А-я]+', _)])

    Args:
        df_columns (list): column names
    Returns:
        pd.DataFrame: готовый к инсерту DF
    """
    df.columns = [re.sub(r'[^A-Za-z_0-9]', 'с', cn) for cn in df.columns]
    
    columns = [
        'app_name', 'app_version', 'created_at', 'event_name', 'user_id',
        'screen_name', 'product_id', 'city_catalog', 'completed_purchases',
        'total_revenue',]
    col_dict = {
        '_app_name_': 'app_name',
        '_app_version_': 'app_version',
        '_created_at_': 'created_at',
        '_event_name_': 'event_name',
        'user_id': 'user_id',
        'сompleted_purchases': 'completed_purchases'}
    try:
        _ = df.rename(columns=col_dict)[columns]
        # Заодно приведем типы
        _.app_name = _.app_name.astype(str)
        _.app_version = _.app_version.astype(str)
        _.created_at = pd.to_datetime(_.created_at)
        _.event_name = _.event_name.astype(str)
        _.user_id = _.user_id.astype(int)
        _.screen_name = _.screen_name.astype(str)
        _.product_id = _.product_id.astype(str)
        _.city_catalog = _.city_catalog.astype('float').astype('Int64')
        _.completed_purchases = _.completed_purchases.astype(str)
        _.total_revenue = _.total_revenue.astype(int)
        
        # PS Возникли сомнения по поводу nullable str типа в BigQuery,
        # надо ли преобразовывать к  пустой строке - '' или оставлять NaN как строку
        # не стал ничего менять, но в проде уточнил бы.

        logger.info("Колонки переименованы, типы приведены к нужным")
    except Exception as err:
        logger.info("Какая-то ошибка при переименоввании", err)
    return _


def insert(df: pd.DataFrame, table: str, project:str, credentials):
    """Insert data from a Pandas dataframe into Google BigQuery. 

    Args:
        df (pd.DataFrame): Name of Pandas dataframe
        project (str): project tame
        table (srt): Name of BigQuery dataset and table, i.e. competitors.products
        credentials: credentials for client
    """
    with bigquery.Client(project, credentials) as client:
        try:
            client.load_table_from_dataframe(df, table)
            logger.info("Данные были загружены")
        except Exception as err:
            logger.info("Ошибка при загрузке", err)


def etl():
    events = extract_events(EVENTS_TABLE, PROJECT, CREDENTIALS)
    transformed_events = transform_flatten_json(events)
    users_orders = create_dict(PATH)
    resulted_df = resulted_dframe(transformed_events, users_orders)
    _ = rename_final(resulted_df)
    logger.info("Данные загружены")
   

if __name__ == "__main__":
    logger = setup_logger(f'{EVENTS_TABLE}')
    logger.setLevel(logging.INFO)
    
    try:
        etl()
    except Exception as err:
        logger.info("Тут могла бы отправка в телеграмм бота %s", err)
