#!/usr/bin/env python
# coding: utf-8

from datetime import date, timedelta

import pandas as pd
import prestodb

import statsmodels.api as sm

import yaml

from SQLQuery import get_site, get_LOCS, get_category, write_db_append, get_abcd_mysql, get_team, get_status, \
    get_acf_data, get_abcd_postgres

with open('config.yaml', 'r') as yamlfile:
    cfg = yaml.full_load(yamlfile)

postgres_conn = cfg['ConnectionStr']['postgresql'].format(user=cfg['DB_CONFIG']['condev-statistics']['user'],
                                                          passwd=cfg['DB_CONFIG']['condev-statistics']['passwd'],
                                                          host=cfg['DB_CONFIG']['condev-statistics']['host'],
                                                          database=cfg['DB_CONFIG']['condev-statistics']['database'])

mysql_catalog_connect = cfg['ConnectionStr']['mysql'].format(user=cfg['DB_CONFIG']['catalog-connect']['user'],
                                                             passwd=cfg['DB_CONFIG']['catalog-connect']['passwd'],
                                                             host=cfg['DB_CONFIG']['catalog-connect']['host'],
                                                             database=cfg['DB_CONFIG']['catalog-connect']['database'])

mysql_catalog_articles = cfg['ConnectionStr']['mysql'].format(user=cfg['DB_CONFIG']['catalog-articles']['user'],
                                                              passwd=cfg['DB_CONFIG']['catalog-articles']['passwd'],
                                                              host=cfg['DB_CONFIG']['catalog-articles']['host'],
                                                              database=cfg['DB_CONFIG']['catalog-articles']['database'])

mysql_blackboard = cfg['ConnectionStr']['mysql'].format(user=cfg['DB_CONFIG']['content-statistics']['user'],
                                                        passwd=cfg['DB_CONFIG']['content-statistics']['passwd'],
                                                        host=cfg['DB_CONFIG']['content-statistics']['host'],
                                                        database=cfg['DB_CONFIG']['content-statistics'][
                                                            'database'])

mysql_adwords = cfg['ConnectionStr']['mysql'].format(user=cfg['DB_CONFIG']['adwords']['user'],
                                                     passwd=cfg['DB_CONFIG']['adwords']['passwd'],
                                                     host=cfg['DB_CONFIG']['adwords']['host'],
                                                     database=cfg['DB_CONFIG']['adwords']['database'])

datalake_conn = prestodb.dbapi.connect(host=cfg['DB_CONFIG']['datalake']['host'],
                                       port=cfg['DB_CONFIG']['datalake']['port'],
                                       user=cfg['DB_CONFIG']['datalake']['user'],
                                       catalog=cfg['DB_CONFIG']['datalake']['catalog'],
                                       http_scheme=cfg['DB_CONFIG']['datalake']['http_scheme'],
                                       schema=cfg['DB_CONFIG']['datalake']['schema'])

# set float format for dataframes
pd.options.display.float_format = '{:,.2f}'.format

# get the first date of current month and previous month
last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
first_day_of_prev_month = date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)
last_m_date = "'" + first_day_of_prev_month.strftime("%Y-%m-%d") + "'"

first_day_of_current_month = date.today().replace(day=1)
current_m_date = "'" + first_day_of_current_month.strftime("%Y-%m-%d") + "'"

df_abcd_mysql = get_abcd_mysql(mysql_blackboard)
df_abcd_postgre = get_abcd_postgres(postgres_conn)
df_abcd = df_abcd_postgre[df_abcd_postgre['monat'].isin(df_abcd_mysql['monat']) == False]

# update table category_classes_pos
write_db_append('category_classes_pos', mysql_blackboard, df_abcd)

df_category = get_category(mysql_catalog_connect)

df_status = get_status(mysql_catalog_connect)

df_team = get_team(mysql_catalog_articles)

df_verifiedClicks = get_LOCS(datalake_conn, last_m_date, current_m_date)

df_site = get_site(mysql_blackboard)

df_verifiedClicks.drop('type', inplace=True, axis=1)
df_clicks = df_verifiedClicks.groupby(['category_id', 'date', 'site_id']).sum().reset_index()

# get monthly clicks data
df_clicks['date'] = pd.to_datetime(df_clicks['date'])
df_clicks['month_year'] = df_clicks['date'].dt.to_period('M')

df_clicks_monthly = df_clicks.groupby(['category_id', 'month_year', 'site_id']).sum('clicks').reset_index()

df_clicks_monthly['date'] = df_clicks_monthly['month_year'].dt.strftime('%Y-%m-01')
df_clicks_monthly['date'] = pd.to_datetime(df_clicks_monthly['date'])
df_clicks_monthly = df_clicks_monthly.drop(['month_year'], axis=1)

df_joined = pd.merge(df_clicks_monthly, df_category, on='category_id')

df_joined = pd.merge(df_joined, df_team, on='category_id')

df_joined = pd.merge(df_joined, df_site, how='left')

df_joined.rename({'name': 'team_name'}, axis=1, inplace=True)

df_joined = df_joined[
    ['date', 'category_id', 'category_name', 'team_id', 'team_name', 'site_id', 'site_name', 'verified_clicks']]

### ACF
df_hist_data = get_acf_data(mysql_blackboard, last_m_date)

len(df_hist_data) + len(df_joined)

df_hist_data.tail()

df_data = pd.concat([df_hist_data, df_joined])

df_data.tail()

df_pivot = pd.pivot_table(df_data, values='verified_clicks', index=['category_id', 'site_id'], columns=['date'],
                          fill_value=0)

df_timeSeries = pd.DataFrame(df_pivot.to_records())


# some categories have too few data (no clicks in a lot of months)
# to get the auto correlation function

def get_acf_12(ts):
    try:
        acf, ci = sm.tsa.acf(ts, nlags=24, alpha=0.05)
        return acf[12]
    except IndexError as error:
        print(error)


ls_acf_12 = []

n = len(df_timeSeries.columns)
for i in range(len(df_timeSeries)):
    list_data = list(df_timeSeries.iloc[i])[2:n]
    ls_adjusted = [i for i in list_data if i != 0]
    ls_acf_12.append(get_acf_12(ls_adjusted))

df_timeSeries['acf_12'] = ls_acf_12

df_acf = df_timeSeries[['category_id', 'site_id', 'acf_12']]

x = df_data.verified_clicks[df_data.category_id == 1561]

### Seasonal Factor

df_data['year'] = df_data['date'].dt.year
df_data['month'] = df_data['date'].dt.month

df_clicks_yearly = df_data.groupby(['year', 'category_name', 'category_id', 'site_id'], as_index=False).sum('clicks')
df_clicks_yearly.drop(['month'], axis=1, inplace=True)

unique_cat = []
cat_list = list(df_data.category_id)
unique_1 = [unique_cat.append(x) for x in cat_list if x not in unique_cat]

unique_site = []
site_list = list(df_data.site_id)
unique_2 = [unique_site.append(x) for x in site_list if x not in unique_site]

# create dictionary for categories, sites and years with clicks in the whole year
cs_dict = {}
for cat_id in unique_cat:
    for site_id in unique_site:
        ser = df_data[(df_data.category_id == cat_id) & (df_data.site_id == site_id)]['year'].value_counts()
        seq_dict = ser.to_dict()
        year_list = []
        for year, month_number in seq_dict.items():
            if month_number == 12:
                year_list.append(year)
        d = {(cat_id, site_id): year_list}
        cs_dict.update(d)

# convert dict to dataframe
df_supp = pd.DataFrame()
df_supp['cs'] = cs_dict.keys()
df_supp['list_year'] = cs_dict.values()

df_supp['category_id'] = df_supp.apply(lambda row: row.cs[0], axis=1)
df_supp['site_id'] = df_supp.apply(lambda row: row.cs[1], axis=1)

df_supp = df_supp[['category_id', 'site_id', 'list_year']]

# get only categories, sites and years with whole year clicks
df_clicks_fullyear = pd.merge(df_clicks_yearly, df_supp, on=['category_id', 'site_id'], how='left')
df_clicks_fullyear['dummy'] = df_clicks_fullyear.apply(lambda row: 1 if row.year in row.list_year else 0, axis=1)

df_clicks_fullyear = df_clicks_fullyear[df_clicks_fullyear['dummy'] == 1]

df_clicks_fullyear.drop(['list_year', 'dummy', 'team_id'], axis=1, inplace=True)
df_clicks_fullyear.rename({'verified_clicks': 'wholeyear_clicks'}, axis=1, inplace=True)

df_seasonal_factor = pd.merge(df_data, df_clicks_fullyear, how='right')

df_seasonal_factor['avg_year'] = df_seasonal_factor['wholeyear_clicks'] / 12
df_seasonal_factor['seasonal_factor'] = df_seasonal_factor['verified_clicks'] / df_seasonal_factor['avg_year']

df_acf_season = pd.merge(df_seasonal_factor, df_acf, how='left')

df_all = pd.merge(df_data, df_acf_season, how='left')

df_data_full = df_all[['date', 'category_id', 'category_name', 'team_id', 'team_name',
                       'site_id', 'site_name', 'year', 'month', 'verified_clicks',
                       'wholeyear_clicks', 'avg_year', 'seasonal_factor', 'acf_12']]

df_append = df_data_full[df_data_full.date == last_m_date]

write_db_append('category_trend_monitoring_monthly', mysql_blackboard, df_append)
