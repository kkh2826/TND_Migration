# Create your views here.
import pandas
from rest_framework.views import APIView
from django.http import JsonResponse

from FACTORY.query import GetColumnInfoDataQuery_MSSQL, GetDBBasicInfoDataQuery_MSSQL, GetSampeDataQuery_MSSQL
from FACTORY.classes import DBMS
from FACTORY.method import GetDBInfo


class TNDDBConnection(APIView):

    def get(self, request):
        result = {
            'SCHEMA_LIST': dict(),
            'ConnectionSuccess': False,
            'QueryState': True,
            'Message': ''
        }
        connectionObject = None

        dbInfo = GetDBInfo(request)

        # DB 연결
        dbmsObj = DBMS(dbInfo.dbms, dbInfo.server, dbInfo.port, dbInfo.username, dbInfo.password, dbInfo.database)
        connectionObject = dbmsObj.Connect()

        connectionSuccess = True if connectionObject is not None else False
        message = 'DB 연결 실패' if connectionObject is None else ''

        result['ConnectionSuccess'] = connectionSuccess
        result['Message'] = message

        # 연결 스키마 / 테이블 정보 가져오기
        query = GetDBBasicInfoDataQuery_MSSQL()

        try:
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

        # DB 연결 끊기
        dbmsObj.Close()

        return JsonResponse(result)


class TNDDBTableData(APIView):

    def get(self, request):
        result = {}
        
        dbInfo = GetDBInfo(request)

        dbmsObj = DBMS(dbInfo.dbms, dbInfo.server, dbInfo.port, dbInfo.username, dbInfo.password, dbInfo.database)
        connectionObject = dbmsObj.Connect()

        schema = request.data['schema']
        table = request.data['table']

        query = GetSampeDataQuery_MSSQL(schema, table)

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

        dbInfo = GetDBInfo(request)

        schema = request.data['schema']
        table = request.data['table']

        query = GetColumnInfoDataQuery_MSSQL(schema, table)


        try:
            dbmsObj = DBMS(dbInfo.dbms, dbInfo.server, dbInfo.port, dbInfo.username, dbInfo.password, dbInfo.database)
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