# ### Seasonal Calender (only run once at EndOfYear)
import yaml
import pandas as pd
from SQLQuery import get_data_calendar, write_db_replace
import numpy as np

with open('config.yaml', 'r') as yamlfile:
    cfg = yaml.full_load(yamlfile)

mysql_blackboard = cfg['ConnectionStr']['mysql'].format(user=cfg['DB_CONFIG']['content-statistics']['user'],
                                                        passwd=cfg['DB_CONFIG']['content-statistics']['passwd'],
                                                        host=cfg['DB_CONFIG']['content-statistics']['host'],
                                                        database=cfg['DB_CONFIG']['content-statistics'][
                                                            'database'])

df_data_full = get_data_calendar(mysql_blackboard)

seasonal_pivot = pd.pivot_table(df_data_full, columns='month', index=['team_id', 'category_id', 'site_id'],
                                values='seasonal_factor')

seasonal_pivot = seasonal_pivot.dropna()
df_seasonal_prep = pd.DataFrame(seasonal_pivot.to_records())

month_value = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
month_name = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
              'November', 'December']

df = df_data_full[['category_id', 'site_id']].drop_duplicates()

ls_month = []
for i in range(len(df)):
    for j in range(len(month_name)):
        ls_month.append(month_name[j])

month_dict = dict(zip(ls_month, month_value))

cat_id = df.category_id.tolist()
ls_cat = np.repeat(cat_id, 12)

s_id = df.site_id.tolist()
ls_site = np.repeat(s_id, 12)

df_calender = pd.DataFrame({'category_id': ls_cat, 'site_id': ls_site, 'month': ls_month})


# get_mean returns null value if there exists no seasonal factor for the category on the website in the expected month
def get_mean(cat, site_id, month):
    month_val = str(month_dict[month])
    x = df_seasonal_prep[month_val][(df_seasonal_prep.category_id == cat) & (df_seasonal_prep.site_id == site_id)]
    try:
        return x.iloc[0]
    except IndexError:
        return np.nan


df_calender['mean_seasonal_factor'] = df_calender.apply(lambda row: get_mean(row.category_id, row.site_id, row.month),
                                                        axis=1)


# nan-values since no seasonal factor for the team on the website exists
def get_quantile(team_id, site_id):
    df_tmp = df_data_full[(df_data_full.team_id == team_id) & (df_data_full.site_id == site_id)]
    season = df_tmp.seasonal_factor
    q70 = np.nanquantile(season, 0.70)
    q85 = np.nanquantile(season, 0.85)
    q95 = np.nanquantile(season, 0.95)
    return [q70, q85, q95]


df_quantile = df_data_full[['team_id', 'team_name', 'site_id', 'site_name']].drop_duplicates()

df_quantile['quantile_70'] = df_quantile.apply(lambda row: get_quantile(row.team_id, row.site_id)[0], axis=1)

df_quantile['quantile_85'] = df_quantile.apply(lambda row: get_quantile(row.team_id, row.site_id)[1], axis=1)
df_quantile['quantile_95'] = df_quantile.apply(lambda row: get_quantile(row.team_id, row.site_id)[2], axis=1)

df_2 = df_data_full[['category_id', 'category_name', 'team_id', 'site_id']].drop_duplicates()

df_quantile = pd.merge(df_quantile, df_2, how='left')


df_seasonal_calender = pd.merge(df_calender, df_quantile, how='left')


def get_season(x, low, medium, high):
    if low <= x < medium:
        return 'low'
    if medium <= x < high:
        return 'medium'
    if x >= high:
        return 'high'
    else:
        return 'no'


df_seasonal_calender['seasonality'] = df_seasonal_calender.apply(
    lambda row: get_season(row.mean_seasonal_factor, row.quantile_70, row.quantile_85, row.quantile_95), axis=1)

df_seasonal_calender = df_seasonal_calender[['team_id',
                                             'team_name', 'category_id', 'category_name', 'site_id', 'site_name',
                                             'month',
                                             'mean_seasonal_factor', 'quantile_70', 'quantile_85', 'quantile_95',
                                             'seasonality']]

write_db_replace('category_trend_monitoring_calender', mysql_blackboard, df_seasonal_calender)
