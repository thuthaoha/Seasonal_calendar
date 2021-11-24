import pandas as pd

import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql
import prestodb
import psycopg2

def read_db_dl(connection: str, sql_query: str):
    '''
    Args:
        connection: Data Lake connection
        sql_query:

    Returns: Dataframe
    '''
    cur = connection.cursor()
    cur.execute(f'{sql_query}')
    records = cur.fetchall()

    df_header = []
    for i in range(len(cur.description)):
        df_header.append(cur.description[i][0])

    df_data = pd.DataFrame(records, columns=df_header)

    cur.close()
    connection.close()
    return df_data

def read_db(connection: str, sql_query: str):
    '''
    Args:
        connection: Mysql/Postgres connection
        sql_query:

    Returns: Dataframe
    '''
    try:
        engine = create_engine(connection)
        data = pd.read_sql(sql_query, engine)
        return data
    except mysql.connector.Error as err:
        print(err)

def write_db_append(table_name, connection, data):
    try:
        engine = create_engine(connection)
        data.to_sql(table_name, engine, if_exists='append', index=False)
    except mysql.connector.Error as error:
        print(error)
        # logger.info(msg=f"""Unable to save volumes for keyword_id {data['keyword_id'].iloc[-1]}""")
    else:
        print('*** items successfully inserted ***')


def write_db_replace(table_name, connection, data):
    try:
        engine = create_engine(connection)
        data.to_sql(table_name, engine, if_exists='replace', index=False)
    except mysql.connector.Error as error:
        print(error)
        # logger.info(msg=f"""Unable to save volumes for keyword_id {data['keyword_id'].iloc[-1]}""")
    else:
        print('*** items successfully inserted ***')

def get_abcd_postgres(conn: str):
    query = f'''
    SELECT * 
    FROM abcd.category_classes
    '''
    return read_db(conn, query)

def get_abcd_mysql(conn: str):
    query = f'''
    SELECT * 
    FROM blackboard.category_classes_pos
    '''
    return read_db(conn, query)

def get_category(conn: str):
    query = f'''
    SELECT
    id as category_id,
    (CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(context_plural_names,'$.de_DE')) IS NULL 
    THEN
    JSON_UNQUOTE(JSON_EXTRACT(context_plural_names,'$.global'))
    ELSE
    JSON_UNQUOTE(JSON_EXTRACT(context_plural_names,'$.de_DE'))
    END) AS category_name
    FROM
    catalog_connect.category
    '''
    return read_db(conn, query)

def get_status(conn: str):
    query = f'''
    SELECT id as category_id
    ,status as Status
    ,context_singular_names as category
    ,online_countries as OnlineCountries
    FROM catalog_connect.category
    '''
    return read_db(conn, query)

def get_team(conn: str):
    query = f'''
    SELECT tc.category_id, tc.team_id, t.name
    FROM catalog_users.team_categories tc
    LEFT JOIN catalog_users.teams t
    ON tc.team_id = t.id
    '''
    return read_db(conn, query)
def get_site(conn: str):
    query = f'''
    SELECT *
    FROM blackboard.sites
    '''
    return read_db(conn, query)

def get_LOCS(conn: str, last_date: str, current_date: str):
    query = f'''
    SELECT 
        date,
        category_id,
        type,
        site_id,
        COUNT(click_id) AS verified_clicks          
    FROM dl_leadouts_prod.verified_clicks
    WHERE DATE("date") >= DATE('''+ last_date +''') 
    AND DATE("date") < DATE('''+ current_date +''')
    AND site_id in (1, 2, 3, 4, 10, 11)
    GROUP BY 1,2,3,4
    '''
    return read_db_dl(conn, query)

def get_acf_data(conn: str, last_date: str):
    query = f'''
    SELECT date
    ,category_id
    ,category_name
    ,team_id
    ,team_name
    ,site_id
    ,site_name
    ,verified_clicks 
    FROM blackboard.category_trend_monitoring_monthly
    WHERE date < DATE('''+ last_date +''') 
    '''
    return read_db(conn, query)

def get_data_calendar(conn: str):
    query = f'''
    SELECT *
    FROM blackboard.category_trend_monitoring_monthly
    '''
    return read_db(conn, query)



