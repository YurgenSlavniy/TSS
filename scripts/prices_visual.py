# НЕОБХОДИМО НАПИСАТЬ СКРИПТ:
# Обращаемся к собранной таблице, переводим в датафрейм и делаем визуализацию Цена от времени. 
# График цены. для начала для одной валютной пары


import pandas as pd
from datetime import datetime
import sqlite3 as sl
import matplotlib.pyplot as plt


# Обращаемся к базе данных и получаем данные по конкретной валютной паре.  
def database():
    database = sl.connect('rub_cur_price.db')
    df = pd.read_sql_query("SELECT * FROM price_list WHERE cur_pair='BTC_RUB'", database)
    df['datetime'] = pd.to_datetime(df['datetime'], format='%d.%m.%y %H:%M')    
    return df

# Визуализация данных. График цена от времени с подписанными осями и названием графика - валютная пара.
def visual(df): 
    # Строим график
    plt.plot(df['datetime'], df['price'])
    # Настройка осей
    title = str(df['cur_pair'][0])
    plt.title(title)
    plt.xlabel('Время')
    plt.ylabel('Цена')

    # Отображение графика
    return plt.show()

# список датафреймов по всем рублёвым валютным парам
def cur_pairs_df():

    rub_pair_list = ['DAI_RUB', 
                 'DOGE_RUB', 
                 'EXM_RUB', 
                 'SHIB_RUB', 
                 'XLM_RUB', 
                 'XRP_RUB', 
                 'XTZ_RUB', 
                 'BTC_RUB',
                 'LTC_RUB',
                 'ETH_RUB',
                 'DASH_RUB',
                 'ZEC_RUB',
                 'WAVES_RUB',
                 'USDT_RUB',
                 'ETC_RUB',
                 'BCH_RUB',
                 'TRX_RUB',
                 'NEO_RUB',
                 'GUSD_RUB',
                 'ALGO_RUB']

    df_list = []   
   
    for el in rub_pair_list:
        database = sl.connect('rub_cur_price.db')
        
        query = str("SELECT * FROM price_list WHERE cur_pair='" + el + "'")
        df = pd.read_sql_query(query, database)
        df['datetime'] = pd.to_datetime(df['datetime'], format='%d.%m.%y %H:%M')    
        df_list.append(df)  
        
    return df_list 
    
    
###############################

df = database()
cur_pairs_df = cur_pairs_df()

for el in cur_pairs_df:
	visual(el)

