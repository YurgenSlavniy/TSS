from django.urls import path
from site_app import views
 
urlpatterns = [
    path("", views.index),
    path("stock/", views.stock),
    path("history/", views.history),
    path("calculator/", views.calculator),
    path("models/", views.models),
