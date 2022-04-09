
# Create your views here.
import pymssql
from rest_framework.views import APIView
from django.http import JsonResponse


class TNDDBConnection(APIView):
    def ConnectDB_MSSQL(self, request):

        connection = None

        server = request.data['server']
        port = request.data['port']
        username = request.data['username']
        password = request.data['password']
        database = request.data['database']
        
        try:
            connection = pymssql.connect(server=server, port=port, user=username, password=password, database=database)
        except:
            connection = None

        return connection


    def get(self, request, dbms):

        result = {}
        connectionObject = None

        if dbms.upper() == 'MSSQL':
            connectionObject = self.ConnectDB_MSSQL(request)

        connectionSuccess = True if connectionObject is not None else False
        message = 'DB 연결 성공' if connectionObject is not None else 'DB 연결 실패'

        result['ConnectionSuccess'] = connectionSuccess
        result['Message'] = message

        return JsonResponse(result)

    