from django.urls import path
from .views import TNDDBConnection

urlpatterns = [
    path('connect/<str:dbms>', TNDDBConnection.as_view()),
]