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

        print(result)

        return JsonResponse(result)


class TNDDBInfo(APIView):
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

    def get(self, request):

        result = {
            'SCHEMA_LIST': dict(),
            'ConnectionSuccess': False,
            'Message': ''
        }
        cursor = None
        connectionObject = self.ConnectDB_MSSQL(request)

        if connectionObject is None:
            result['ConnectionSuccess'] = False
            result['Message'] = 'DB 연결 실패'
        else:
            result['ConnectionSuccess'] = True
            result['Message'] = 'DB 연결 성공'

        query = '''
SELECT TABLE_SCHEMA AS SCHEMA_LIST
     , TABLE_NAME
     , (SELECT I.rows AS ROW_CNT
		  FROM sysindexes I INNER JOIN sysobjects O ON I.id = O.id
		 WHERE I.indid < 2 
		   AND O.xtype = 'U'
		   AND O.name = a.TABLE_NAME) AS ROW_CNT
  FROM INFORMATION_SCHEMA.TABLES A
 WHERE 1=1
   AND TABLE_TYPE = 'BASE TABLE'
ORDER BY 1,2
'''

        try:
            cursor = connectionObject.cursor(as_dict=True)
            cursor.execute(query)
        except:
            result['QueryState'] = False
            result['Message'] = 'DB 쿼리 실행 실패'

        datas = cursor.fetchall()

        for data in datas:
            schema = data['SCHEMA_LIST']
            table = data['TABLE_NAME']
            rowCnt = data['ROW_CNT']

            if schema not in result['SCHEMA_LIST'].keys():
                result['SCHEMA_LIST'].setdefault(schema, {'TABLE_NAME': dict()})
                result['SCHEMA_LIST'][schema]['TABLE_NAME'].setdefault(table, rowCnt)
            else:
                if table not in result['SCHEMA_LIST'][schema]['TABLE_NAME'].keys():
                    result['SCHEMA_LIST'][schema]['TABLE_NAME'].setdefault(table, rowCnt)

        return JsonResponse(result)

    