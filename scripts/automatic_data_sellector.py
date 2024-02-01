# НЕОБХОДИМО НАПИСАТЬ СКРИПТ:
# Каждый час скрипт сохраняет информацию полученную с биржи exmo.me
# все торговые пары, которые торгуются с рублём и цены, в базу данных. 

import pandas as pd
import requests
from datetime import datetime
import sqlite3 as sl
import schedule


# Апиай запрос и возвращение датафрейма по валютной паре. 
def order_book_df(cur_pair):
    url = "https://api.exmo.me/v1.1/order_book"

    payload='pair=' + cur_pair + '&limit=100'
    headers = {
      'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    df = pd.read_json(response.text, orient='records')
    cur_pair = df.columns.tolist()[0]
    
    curtent_pair = []
    ask = []
    ask_amount = []
    ask_quantity = []
    ask_top = []
    bid = []
    bid_amount = []
    bid_quantity = []
    bid_top = []
    
    curtent_pair.append(cur_pair)
    ask.append(df.T['ask'].tolist()[0])
    ask_amount.append(df.T['ask_amount'].tolist()[0])
    ask_quantity.append(df.T['ask_quantity'].tolist()[0])
    ask_top.append(df.T['ask_top'].tolist()[0])
    bid.append(df.T['bid'].tolist()[0])
    bid_amount.append(df.T['bid_amount'].tolist()[0])
    bid_quantity.append(df.T['bid_quantity'].tolist()[0])
    bid_top.append(df.T['bid_top'].tolist()[0])
    
    order_book_df = pd.DataFrame({'cur_pair' : curtent_pair,
                                  'ask': ask,
                                  'ask_amount': ask_amount,
                                  'ask_quantity':  ask_quantity,
                                  'ask_top':  ask_top,
                                  'bid':  bid,
                                  'bid_amount':  bid_amount,
                                  'bid_quantity':  bid_quantity,
                                  'bid_top':  bid_top
                         },
                           columns=['cur_pair',
                                  'ask',
                                  'ask_amount',
                                  'ask_quantity',
                                  'ask_top',
                                  'bid',
                                  'bid_amount',
                                  'bid_quantity',
                                  'bid_top'
                                   ])
    return order_book_df

# возвращает датафрейм из 3 ёх столбцов. Валютная пара, цена, датавремя. 
def price_list():
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

    currents_list = []
    price_list = []
    date_time_list = []

    for el in rub_pair_list:
        a = order_book_df(el)
        currents_list.append(a['cur_pair'][0])
        price_list.append((float(a['ask_top'][0]) + float(a['bid_top'][0]))/2)
        date_time_list.append(datetime.now().strftime("%d.%m.%y %H:%M"))
    	
    price_list_df = pd.DataFrame({'cur_pair': currents_list,  
                              'price':price_list,
                               'date_time':date_time_list }, 
                            columns=['cur_pair', 'price', 'date_time'])

    return price_list_df

# Функция создаёт базу данных и таблицу
def database():
    database = sl.connect('rub_cur_price.db')

    with database:
        data = database.execute("select count(*) from sqlite_master where type='table' and name='price_list'")
        for row in data:
            if row[0] == 0:
                with database:
                    database.execute("""
                   CREATE TABLE price_list(
    	    		cur_pair VARCHAR(20),
    	    		price FLOAT,
    	    		datetime DATETIME
                    );
                """)
    
    return database 
  
# Добавляем данные в базу данных 
def data_to_dtabase():
    database = sl.connect('rub_cur_price.db')
    df = price_list()
    sql = 'INSERT INTO price_list (cur_pair, price, datetime) values(?, ?, ?)'
    data = []
    idxs = range(0, len(df))
    for el in idxs:
        data.append((df.iloc[el][0], df.iloc[el][1], df.iloc[el][2]))
    with database:
        database.executemany(sql, data)
    
    database_curs = database.cursor()     
    database_curs.execute("select count(*) from price_list") # делаем запос на кол-во строк в таблице
    row_count = database_curs.fetchall()
    print(f'rows: {row_count} ', datetime.now().strftime("%d.%m.%y %H:%M"))
    return database 

    
#####################

database = database()
schedule.every(15).minutes.do(data_to_dtabase)

while True:
    schedule.run_pending()

    
