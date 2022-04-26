from django.urls import path
from .views import TNDDBConnection, TNDDBInfo, TNDDBTableData, TNDColumnInfo

urlpatterns = [
    path('connect/', TNDDBConnection.as_view()),
    path('databaseinfo/', TNDDBInfo.as_view()),
    path('tabledata/', TNDDBTableData.as_view()),
    path('columninfo/', TNDColumnInfo.as_view())
]