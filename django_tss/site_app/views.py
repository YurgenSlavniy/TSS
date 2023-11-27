from django.shortcuts import render
import pandas as pd
import requests

def ticker():
    url = "https://api.exmo.com/v1.1/ticker"

    payload={}
    headers = {
      'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    df = pd.read_json(response.text, orient='records').T
    
    curent_pairs_list = df.T.columns.values.tolist ()
    buy_price_list = df['buy_price'].tolist()
    sell_price_list = df['sell_price'].tolist()
    last_trade_list = df['last_trade'].tolist()
    high_list = df['high'].tolist()
    low_list = df['low'].tolist()
    avg_list = df['avg'].tolist()
    vol_list = df['vol'].tolist()
    vol_curr_list = df['vol_curr'].tolist()
    
    ticker_df = pd.DataFrame({'cur_pair' : curent_pairs_list,
                          'buy_price' : buy_price_list,
                          'sell_price': sell_price_list,
                          'last_trade': last_trade_list,
                          'high': high_list,
                          'low': low_list, 
                          'avg': avg_list, 
                          'vol': vol_list, 
                          'vol_curr': vol_curr_list 
                              
                         },
                           columns=['cur_pair',
                          'buy_price',
                          'sell_price',
                          'last_trade',
                          'high',
                          'low', 
                          'avg', 
                          'vol', 
                          'vol_curr' 
                                   ])

    
    return ticker_df[['cur_pair','last_trade']]



# главная страница
def index(request):
	return render(request, 'index.html')
# биржевые сводки по апиай получаемые 
# и в красивом виде пользователю представленные
def stock(request):
    return render(request, "stock.html", context={'api':ticker()})
# аналитика истори  торгов, загружаемой в формате .csv
def history(request):
    return render(request, "history.html")
# калькулятор ордеров, расчитывает 
# расстановку ордеров в зависимости от введёных пользователем параметров
def calculator(request):
    return render(request, "calculator.html")
# Модели поведения рынка с расставленными ордерами, 
# прогнозирование прибылей и убытков
def models(request):
    return render(request, "models.html")
