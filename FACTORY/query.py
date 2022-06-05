from FACTORY.method import MakeColumnQueryStatement


'''
    연결된 DB의 스키마 리스트 / 테이블 리스트 / 테이블의 데이터 건 수 (MSSQL)
'''
def GetDBBasicInfoDataQuery_MSSQL():
    sql = '''
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

    return sql
    

'''
    1000개의 데이터 가져오기 (MSSQL)
'''
def GetSampleDataQuery_MSSQL(schema, table, columnInfoDatas):
    columnInfo = MakeColumnQueryStatement('MSSQL', columnInfoDatas)
    sql = '''
SELECT TOP 1000 {columnInfo}
  FROM {schema}.{table} 
        '''.format(columnInfo=columnInfo, schema=schema, table=table)


    return sql


'''
    선택한 Table에 대한 Column 정보 가져오기 (MSSQL)
'''
def GetColumnInfoDataQuery_MSSQL(schema, table):
    sql = ''' 
SELECT CAST( TAB.object_id AS VARCHAR ) AS OBJECT_ID
     , CAST( TAB.NAME AS VARCHAR ) AS TABLE_ID 
	 , CAST( TAB_COMMENT.VALUE AS VARCHAR )  AS TABLE_NAME
	 , CAST( COL.NAME AS VARCHAR ) AS COLUMN_ID
	 , CAST( COL_COMMENT.VALUE AS VARCHAR ) AS COLUMN_NAME
	 , CAST( COL.COLUMN_ID AS VARCHAR ) AS COLUMN_NO
	 , CAST( CASE WHEN COL.USER_TYPE_ID IN (41, 42, 43)  THEN TYPE.NAME + '(' + CAST(COL.SCALE AS VARCHAR) + ')' -- TIME, DATETIME2, DATETIMEOFFSET
				  WHEN COL.USER_TYPE_ID IN (175, 231, 239, 167, 165, 173) THEN TYPE.NAME + '(' + IIF(COL.MAX_LENGTH = -1, 'MAX', CAST(COL.MAX_LENGTH AS VARCHAR)) + ')'
				  WHEN COL.USER_TYPE_ID IN (106, 108) THEN TYPE.NAME + '(' + CAST(COL.PRECISION AS VARCHAR) + ',' + CAST(COL.SCALE AS VARCHAR) + ')' -- DECIMAL, NUMERIC
				  ELSE TYPE.NAME
	         END AS VARCHAR ) COL_TYPE
     , CASE WHEN COL.is_nullable = 0 THEN 'NOT NULL'
            ELSE 'NULL'
       END AS NULL_YN
     , CASE WHEN (SELECT 'Y'
                    FROM sys.objects Z
				       , INFORMATION_SCHEMA.KEY_COLUMN_USAGE E
				   WHERE SCHEMA_NAME(TAB.schema_id) = E.TABLE_SCHEMA
                     AND TAB.name = E.TABLE_NAME
                     AND COL.name = E.COLUMN_NAME
					 AND Z.name = E.CONSTRAINT_NAME
					 AND Z.TYPE_DESC = 'PRIMARY_KEY_CONSTRAINT') = 'Y' THEN 'PK'
            ELSE ''
       END AS PK
     , CASE WHEN (SELECT 'Y'
                    FROM sys.objects Z
				       , INFORMATION_SCHEMA.KEY_COLUMN_USAGE E
				   WHERE SCHEMA_NAME(TAB.schema_id) = E.TABLE_SCHEMA
                     AND TAB.name = E.TABLE_NAME
                     AND COL.name = E.COLUMN_NAME
					 AND Z.name = E.CONSTRAINT_NAME
					 AND Z.TYPE_DESC = 'FOREIGN_KEY_CONSTRAINT') = 'Y' THEN 'FK'
            ELSE ''
       END AS FK
	 , CASE WHEN (SELECT 'Y'
                    FROM sys.objects Z
				       , INFORMATION_SCHEMA.KEY_COLUMN_USAGE E
				   WHERE SCHEMA_NAME(TAB.schema_id) = E.TABLE_SCHEMA
                     AND TAB.name = E.TABLE_NAME
                     AND COL.name = E.COLUMN_NAME
					 AND Z.name = E.CONSTRAINT_NAME
					 AND Z.TYPE_DESC = 'UNIQUE_CONSTRAINT') = 'Y' THEN 'UQ'
            ELSE ''
       END AS UQ
	 , CAST( FK.referenced_object AS VARCHAR ) AS REFERENCED_OBJECT 
	 , CAST( FK.referenced_column_name AS VARCHAR ) AS REFERENCED_COLUMN_NAME
	 , CASE WHEN TYPE.NAME IN ('hierarchyid', 'varbinary', 'binary') THEN 'Y'
			ELSE 'N'
	   END AS BYTE_YN -- 바이트타입여부
  FROM sys.objects AS TAB 
  LEFT OUTER JOIN sys.all_columns COL ON TAB.object_id = COL.object_id
  LEFT OUTER JOIN sys.types TYPE ON COL.user_type_id = TYPE.user_type_id
  LEFT OUTER JOIN SYS.extended_properties AS TAB_COMMENT ON TAB.OBJECT_ID = TAB_COMMENT.MAJOR_ID
														AND TAB_COMMENT.MINOR_ID = 0
  LEFT OUTER JOIN SYS.extended_properties AS COL_COMMENT ON COL.OBJECT_ID = COL_COMMENT.MAJOR_ID
														AND COL.COLUMN_ID = COL_COMMENT.MINOR_ID
														AND COL_COMMENT.class_desc = 'OBJECT_OR_COLUMN'
  LEFT OUTER JOIN ( SELECT F.parent_object_id
                         , f.name AS foreign_key_name  
						 , OBJECT_NAME(f.parent_object_id) AS table_name  
						 , COL_NAME(fc.parent_object_id, fc.parent_column_id) AS constraint_column_name  
						 , OBJECT_NAME (f.referenced_object_id) AS referenced_object  
						 , COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS referenced_column_name  
						 , is_disabled  
						 , delete_referential_action_desc  
						 , update_referential_action_desc  
                      FROM sys.foreign_keys AS F 
					  INNER JOIN sys.foreign_key_columns AS FC ON f.object_id = fc.constraint_object_id 
                    ) AS FK ON COL.object_id = FK.parent_object_id
					       AND COL.name = FK.constraint_column_name
 WHERE TAB.object_id = OBJECT_ID('{schema}.{table}')
        '''.format(schema=schema, table=table)

    print(sql)

    return sql



'''
    연결된 DB의 스키마 리스트 / 테이블 리스트 / 테이블의 데이터 건 수 (POSTGRESQL)
'''
def GetDBBasicInfoDataQuery_POSTGRESQL():
    sql = '''
SELECT A.TABLE_SCHEMA AS "SCHEMA_LIST"
     , A.TABLE_NAME AS "TABLE_NAME"
	 , B.N_LIVE_TUP AS "ROW_CNT"
  FROM INFORMATION_SCHEMA.TABLES A
  	   LEFT OUTER JOIN PG_STAT_ALL_TABLES B ON A.TABLE_SCHEMA = B.SCHEMANAME
	   									   AND A.TABLE_NAME = B.RELNAME
 WHERE TABLE_TYPE = 'BASE TABLE'
ORDER BY 1,2  
    '''

    return sql


'''
    1000개의 데이터 가져오기 (POSTGRESQL)
'''
def GetSampleDataQuery_POSTGRESQL(schema, table, columnInfoDatas):
    columnInfo = MakeColumnQueryStatement('POSTGRESQL', columnInfoDatas)
    sql = '''
SELECT {columnInfo}
  FROM {schema}.{table}
LIMIT 1000
;
    '''.format(columnInfo=columnInfo, schema=schema, table=table)

    return sql


'''
    선택한 Table에 대한 Column 정보 가져오기 (POSTGRESQL)
'''
def GetColumnInfoDataQuery_POSTGRESQL(schema, table):
    sql = '''
SELECT CLS.OID AS "OBJECT_ID"
	 , TAB.TABLE_NAME AS "TABLE_ID"
	 , T_COMMENT.DESCRIPTION AS "TABLE_NAME"
	 , COL.COLUMN_NAME AS "COLUMN_ID"
	 , COL_COMMENT.DESCRIPTION AS "COLUMN_NAME"
	 , COL.ORDINAL_POSITION AS "COLUMN_NO"
	 , CASE WHEN COL.DATA_TYPE LIKE '%character%' THEN COL.DATA_TYPE || '(' || COL.CHARACTER_MAXIMUM_LENGTH || ')'
	   		WHEN COL.DATA_TYPE LIKE '%time%' THEN COL.DATA_TYPE
			WHEN COL.DATA_TYPE IN ('numeric') THEN COL.DATA_TYPE || '(' || COL.NUMERIC_PRECISION || ',' || COL.NUMERIC_SCALE || ')'
			ELSE COL.DATA_TYPE
	   END AS "COL_TYPE"
	 , CASE WHEN COL.IS_NULLABLE = 'YES' THEN 'NULL'
	        ELSE 'NOT NULL' 
       END AS "NULL_YN"
	 , COALESCE(PK.PK_COL, '') AS "PK"
	 , COALESCE(FK.FK_COL, '') AS "FK"
	 , COALESCE(UQ.UQ_COL, '') AS "UQ"
	 , FK.PARENT_TABLE_NAME AS "REFERENCED_OBJECT"
	 , FK.PARENT_COLUMN_NAME AS "REFERENCED_COLUMN_NAME"
	 , CASE WHEN COL.DATA_TYPE = 'bytea' THEN 'Y'
	 		ELSE 'N'
	   END AS "BYTE_YN"
  FROM INFORMATION_SCHEMA.TABLES TAB
  		INNER JOIN PG_CLASS CLS ON CLS.RELNAME = TAB.TABLE_NAME
		INNER JOIN INFORMATION_SCHEMA.COLUMNS COL ON COL.TABLE_CATALOG = TAB.TABLE_CATALOG
												  AND COL.TABLE_SCHEMA = TAB.TABLE_SCHEMA
												  AND COL.TABLE_NAME = TAB.TABLE_NAME
		LEFT OUTER JOIN PG_DESCRIPTION T_COMMENT ON T_COMMENT.OBJOID = CLS.OID
												 AND T_COMMENT.OBJSUBID = 0	-- 테이블COMMENT
		LEFT OUTER JOIN (SELECT Z.OBJOID, Z.OBJSUBID, Z.DESCRIPTION, Y.ATTNAME
						   FROM PG_DESCRIPTION Z
						 		INNER JOIN PG_ATTRIBUTE Y ON Z.OBJOID = Y.ATTRELID
														 AND Z.OBJSUBID = Y.ATTNUM
						 								 AND Z.OBJSUBID != 0 -- 컬럼COMMENT
						 								 AND Y.ATTNUM >= 1) COL_COMMENT ON COL_COMMENT.OBJOID = CLS.OID
														 					 		    AND COL_COMMENT.ATTNAME = COL.COLUMN_NAME
		LEFT OUTER JOIN (SELECT Z.TABLE_CATALOG, Z.TABLE_SCHEMA, Z.TABLE_NAME, Z.CONSTRAINT_NAME, Y.COLUMN_NAME, 'Y' AS PK_COL
						   FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS Z
						 		LEFT OUTER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Y ON Z.CONSTRAINT_TYPE = 'PRIMARY KEY'
						 																	AND Z.TABLE_CATALOG = Y.TABLE_CATALOG
						 																	AND Z.TABLE_SCHEMA = Y.TABLE_SCHEMA
						 																	AND Z.TABLE_NAME = Y.TABLE_NAME
						 																	AND Z.CONSTRAINT_NAME = Y.CONSTRAINT_NAME
						) PK ON PK.TABLE_CATALOG = COL.TABLE_CATALOG
						    AND PK.TABLE_SCHEMA = COL.TABLE_SCHEMA
							AND PK.TABLE_NAME = COL.TABLE_NAME
							AND PK.COLUMN_NAME = COL.COLUMN_NAME
		LEFT OUTER JOIN (SELECT Z.TABLE_CATALOG, Z.TABLE_SCHEMA, Z.TABLE_NAME, Z.CONSTRAINT_NAME, Y.COLUMN_NAME
						      , X.TABLE_NAME AS PARENT_TABLE_NAME, X.COLUMN_NAME AS PARENT_COLUMN_NAME
						 	  , 'Y' AS FK_COL
						  FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS Z
								INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Y ON Y.CONSTRAINT_NAME = Z.CONSTRAINT_NAME	   
						 		INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE X ON X.CONSTRAINT_NAME = Z.CONSTRAINT_NAME
						 WHERE Z.CONSTRAINT_TYPE = 'FOREIGN KEY') FK ON FK.TABLE_CATALOG = COL.TABLE_CATALOG
																	 AND FK.TABLE_SCHEMA = COL.TABLE_SCHEMA
																	 AND FK.TABLE_NAME = COL.TABLE_NAME
																	 AND FK.COLUMN_NAME = COL.COLUMN_NAME
		LEFT OUTER JOIN (SELECT Z.TABLE_CATALOG, Z.TABLE_SCHEMA, Z.TABLE_NAME, Z.CONSTRAINT_NAME, Y.COLUMN_NAME, 'Y' AS UQ_COL
						   FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS Z
						 		LEFT OUTER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Y ON Z.CONSTRAINT_TYPE = 'UNIQUE'
						 																	AND Z.TABLE_CATALOG = Y.TABLE_CATALOG
						 																	AND Z.TABLE_SCHEMA = Y.TABLE_SCHEMA
						 																	AND Z.TABLE_NAME = Y.TABLE_NAME
						 																	AND Z.CONSTRAINT_NAME = Y.CONSTRAINT_NAME
						) UQ ON UQ.TABLE_CATALOG = COL.TABLE_CATALOG
						    AND UQ.TABLE_SCHEMA = COL.TABLE_SCHEMA
							AND UQ.TABLE_NAME = COL.TABLE_NAME
							AND UQ.COLUMN_NAME = COL.COLUMN_NAME
 WHERE TAB.TABLE_TYPE = 'BASE TABLE'
   AND TAB.TABLE_SCHEMA || '.' || TAB.TABLE_NAME = LOWER('{schema}.{table}')
ORDER BY CLS.OID, COL.ORDINAL_POSITION  
    '''.format(schema=schema, table=table)

    return sql