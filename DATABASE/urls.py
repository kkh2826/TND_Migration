from django.urls import path
from .views import TNDDBConnection, TNDDBInfo

urlpatterns = [
    path('connect/<str:dbms>/', TNDDBConnection.as_view()),
    path('databaseinfo/', TNDDBInfo.as_view())
]