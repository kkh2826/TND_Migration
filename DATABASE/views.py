# Create your views here.
import pandas
from rest_framework.views import APIView
from django.http import JsonResponse

from FACTORY.classes import DBMS
from FACTORY.method import GetDBInfo, get_db_basicinfo_data_query, get_columninfo_data_query, get_sample_data_query


class TNDDBConnection(APIView):

    def post(self, request):
        result = {
            'SCHEMA_LIST': dict(),
            'ConnectionSuccess': False,
            'QueryState': False,
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

        if connectionSuccess == False:
            return JsonResponse(result)

        # 연결 스키마 / 테이블 정보 가져오기
        query = get_db_basicinfo_data_query(dbInfo.dbms)

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

    def post(self, request):
        result = {}
        
        dbInfo = GetDBInfo(request)

        dbmsObj = DBMS(dbInfo.dbms, dbInfo.server, dbInfo.port, dbInfo.username, dbInfo.password, dbInfo.database)
        connectionObject = dbmsObj.Connect()

        schema = request.data['schema']
        table = request.data['table']

        # Column 정보를 이용해 BYTE_YN을 통해 Column을 Query에 직접 사용.
        query_get_columninfo = get_columninfo_data_query(dbInfo.dbms, schema, table)

        try:
            columnInfoDatas = dbmsObj.ExecuteQuery(query_get_columninfo)
            query_get_sample = get_sample_data_query(dbInfo.dbms, schema, table, columnInfoDatas)
            print(query_get_sample)
            datas = pandas.read_sql_query(sql=query_get_sample, con=connectionObject)
        except:
            result['QueryState'] = False
            result['Message'] = 'DB 쿼리 실행 실패'
            
            return JsonResponse(result)

        datas = datas.to_dict(orient='records')

        result['data'] = datas

        return JsonResponse(result)


class TNDColumnInfo(APIView):

    def post(self, request):
        result = {
            'object_id': dict(),
            'ConnectionSuccess': False,
            'Message': ''
        }

        dbInfo = GetDBInfo(request)

        schema = request.data['schema']
        table = request.data['table']

        query = get_columninfo_data_query(dbInfo.dbms, schema, table)

        try:
            dbmsObj = DBMS(dbInfo.dbms, dbInfo.server, dbInfo.port, dbInfo.username, dbInfo.password, dbInfo.database)
            datas = dbmsObj.ExecuteQuery(query)
            result['ConnectionSuccess'] = True
        except:
            result['QueryState'] = False
            result['Message'] = 'DB 쿼리 실행 실패'
            
            return JsonResponse(result)

        for data in datas:
            objectId = data['OBJECT_ID']
            tableId = data['TABLE_ID']
            tableName = data['TABLE_NAME']

            columnInfo = dict()
            
            columnInfo['COLUMN_ID'] = data['COLUMN_ID']
            columnInfo['COLUMN_NAME'] = data['COLUMN_NAME']
            columnInfo['COLUMN_NO'] = data['COLUMN_NO']
            columnInfo['COL_TYPE'] = data['COL_TYPE']
            columnInfo['NULL_YN'] = data['NULL_YN']
            columnInfo['PK'] = data['PK']
            columnInfo['FK'] = data['FK']
            columnInfo['UQ'] = data['UQ']
            columnInfo['referenced_object'] = data['REFERENCED_OBJECT']
            columnInfo['referenced_column_name'] = data['REFERENCED_COLUMN_NAME']

            if objectId not in result['object_id'].keys():
                result['object_id'].setdefault(objectId, {'TABLE_ID': dict()})
                result['object_id'][objectId]['TABLE_ID'].setdefault(tableId, {'TABLE_NAME': dict()})
                result['object_id'][objectId]['TABLE_ID'][tableId]['TABLE_NAME'].setdefault(tableName, [])
                result['object_id'][objectId]['TABLE_ID'][tableId]['TABLE_NAME'][tableName].append(columnInfo)
            else:
                result['object_id'][objectId]['TABLE_ID'][tableId]['TABLE_NAME'][tableName].append(columnInfo)


        return JsonResponse(result)