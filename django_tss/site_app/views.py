from django.shortcuts import render
# главная страница
def index(request):
	return render(request, 'index.html')
# биржевые сводки по апиай получаемые 
# и в красивом виде пользователю представленные
def stock(request):
    return render(request, "stock.html")
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
