# Create your views here.
import pymssql
import pandas
from rest_framework.views import APIView
from django.http import JsonResponse

from FACTORY.query import GetDBBasicInfoDataQuery, GetSampeDataQuery, GetColumnInfoDataQuery

'''
    DB 연결 객체
'''
class DBInfo:
    def __init__(self, server, port, username, password, database):
        self.server = server
        self.port = port
        self.username = username
        self.password = password
        self.database = database


'''
    MSSQL 클래스
'''
class MSSQL:
    def __init__(self, server, port, username, password, database):
        self.server = server
        self.port = port
        self.username = username
        self.password = password
        self.database = database

    def Connect(self):
        try:
            self.conn = pymssql.connect(server=self.server, port=self.port, user=self.username, password=self.password, database=self.database)
        except:
            return None

        return self.conn

    def ExecuteQuery(self, query):

        cursor = None

        connectionObject = self.Connect()

        try:
            cursor = connectionObject.cursor(as_dict=True)
            cursor.execute(query)
        except:
            return None

        data = cursor.fetchall()

        return data

def GetDBInfo(self):
    server = self.data['server']
    port = self.data['port']
    username = self.data['username']
    password = self.data['password']
    database = self.data['database']

    dbInfo = DBInfo(server, port, username, password, database)

    return dbInfo


class TNDDBConnection(APIView):

    def get(self, request, dbms):
        result = {}
        connectionObject = None

        dbInfo = GetDBInfo(request)

        if dbms.upper() == 'MSSQL':
            dbmsObj = MSSQL(dbInfo.server, dbInfo.port, dbInfo.username, dbInfo.password, dbInfo.database)
            connectionObject = dbmsObj.Connect()

        connectionSuccess = True if connectionObject is not None else False
        message = 'DB 연결 성공' if connectionObject is not None else 'DB 연결 실패'

        result['ConnectionSuccess'] = connectionSuccess
        result['Message'] = message

        print(result)

        return JsonResponse(result)


class TNDDBInfo(APIView):

    def get(self, request):

        result = {
            'SCHEMA_LIST': dict(),
            'ConnectionSuccess': False,
            'Message': ''
        }

        dbInfo = GetDBInfo(request)

        query = GetDBBasicInfoDataQuery()

        try:
            dbmsObj = MSSQL(dbInfo.server, dbInfo.port, dbInfo.username, dbInfo.password, dbInfo.database)
            datas = dbmsObj.ExecuteQuery(query)
        except:
            result['QueryState'] = False
            result['Message'] = 'DB 쿼리 실행 실패'
            
            return JsonResponse(result)


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


class TNDDBTableData(APIView):

    def get(self, request):
        result = {}
        
        dbInfo = GetDBInfo(request)

        dbmsObj = MSSQL(dbInfo.server, dbInfo.port, dbInfo.username, dbInfo.password, dbInfo.database)
        connectionObject = dbmsObj.Connect()

        schema = request.data['schema']
        table = request.data['table']

        query = GetSampeDataQuery(schema, table)

        try:
            datas = pandas.read_sql_query(sql=query, con=connectionObject)
        except:
            result['QueryState'] = False
            result['Message'] = 'DB 쿼리 실행 실패'
            
            return JsonResponse(result)

        datas = datas.to_dict(orient='records')

        result['data'] = datas

        return JsonResponse(result)


class TNDColumnInfo(APIView):

    def get(self, request):
        result = {
            'object_id': dict(),
            'ConnectionSuccess': False,
            'Message': ''
        }

        # columnInfo = dict.fromkeys(['COLUMN_ID', 'COLUMN_NAME', 'COLUMN_NO', 'COL_TYPE', 'NULL_YN', 'PK', 'FK', 'UQ', 'referenced_object', 'referenced_column_name'])

        dbInfo = GetDBInfo(request)

        schema = request.data['schema']
        table = request.data['table']

        query = GetColumnInfoDataQuery(schema, table)


        try:
            dbmsObj = MSSQL(dbInfo.server, dbInfo.port, dbInfo.username, dbInfo.password, dbInfo.database)
            datas = dbmsObj.ExecuteQuery(query)
        except:
            result['QueryState'] = False
            result['Message'] = 'DB 쿼리 실행 실패'
            
            return JsonResponse(result)


        for data in datas:
            objectId = data['object_id']
            tableId = data['TABLE_ID']
            tableName = data['TABLE_NAME'].decode('utf-8')

            columnInfo = dict()
            
            columnInfo['COLUMN_ID'] = data['COLUMN_ID']
            columnInfo['COLUMN_NAME'] = data['COLUMN_NAME'].decode('utf-8')
            columnInfo['COLUMN_NO'] = data['COLUMN_NO']
            columnInfo['COL_TYPE'] = data['COL_TYPE']
            columnInfo['NULL_YN'] = data['NULL_YN']
            columnInfo['PK'] = data['PK']
            columnInfo['FK'] = data['FK']
            columnInfo['UQ'] = data['UQ']
            columnInfo['referenced_object'] = data['referenced_object']
            columnInfo['referenced_column_name'] = data['referenced_column_name']

            if objectId not in result['object_id'].keys():
                result['object_id'].setdefault(objectId, {'TABLE_ID': dict()})
                result['object_id'][objectId]['TABLE_ID'].setdefault(tableId, {'TABLE_NAME': dict()})
                result['object_id'][objectId]['TABLE_ID'][tableId]['TABLE_NAME'].setdefault(tableName, [])
                result['object_id'][objectId]['TABLE_ID'][tableId]['TABLE_NAME'][tableName].append(columnInfo)
            else:
                result['object_id'][objectId]['TABLE_ID'][tableId]['TABLE_NAME'][tableName].append(columnInfo)


        return JsonResponse(result)