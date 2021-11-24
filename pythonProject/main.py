from datetime import date, timedelta

import mysql.connector
import numpy as np
import pandas as pd
import prestodb
import statsmodels.api as sm
from sqlalchemy import create_engine

import SQLQuery as sq
import yaml


#config_path = os.environ['CONFIG_PATH']

with open('config.yaml', 'r') as yamlfile:
    cfg = yaml.full_load(yamlfile)


datalake_conn = prestodb.dbapi.connect(host=cfg['DB_CONFIG']['datalake']['host'],
                                       port=cfg['DB_CONFIG']['datalake']['port'],
                                       user=cfg['DB_CONFIG']['datalake']['user'],
                                       catalog=cfg['DB_CONFIG']['datalake']['catalog'],
                                       http_scheme=cfg['DB_CONFIG']['datalake']['http_scheme'],
                                       schema=cfg['DB_CONFIG']['datalake']['schema'])

# get the first date of current month and previous month
last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
first_day_of_prev_month = date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)
last_m_date = "'" + first_day_of_prev_month.strftime("%Y-%m-%d") + "'"
print(last_m_date)

first_day_of_current_month = date.today().replace(day=1)
current_m_date = "'" + first_day_of_current_month.strftime("%Y-%m-%d") + "'"
print(current_m_date)

test = sq.get_LOCS(datalake_conn, last_m_date, current_m_date)
print(test)




