
'''
    연결된 DB의 스키마 리스트 / 테이블 리스트 / 테이블의 데이터 건 수
'''
def GetDBBasicInfoDataQuery():
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
    1000개의 데이터 가져오기
'''
def GetSampeDataQuery(schema, table):
    sql = '''
SELECT TOP 1000 * 
  FROM {schema}.{table} 
        '''.format(schema=schema, table=table)

    return sql


'''
    선택한 Table에 대한 Column 정보 가져오기
'''
def GetColumnInfoDataQuery(schema, table):
    sql = '''
SELECT TAB.object_id
     , TAB.NAME AS TABLE_ID
	 , TAB_COMMENT.VALUE AS TABLE_NAME
	 , COL.NAME AS COLUMN_ID
	 , COL_COMMENT.VALUE AS COLUMN_NAME
	 , COL.COLUMN_ID AS COLUMN_NO
	 , CASE WHEN COL.USER_TYPE_ID IN (41, 42, 43)  THEN TYPE.NAME + '(' + CAST(COL.SCALE AS VARCHAR) + ')' -- TIME, DATETIME2, DATETIMEOFFSET
			WHEN COL.USER_TYPE_ID IN (175, 231, 239, 167, 165, 173) THEN TYPE.NAME + '(' + IIF(COL.MAX_LENGTH = -1, 'MAX', CAST(COL.MAX_LENGTH AS VARCHAR)) + ')'
			WHEN COL.USER_TYPE_ID IN (106, 108) THEN TYPE.NAME + '(' + CAST(COL.PRECISION AS VARCHAR) + ',' + CAST(COL.SCALE AS VARCHAR) + ')' -- DECIMAL, NUMERIC
	        ELSE TYPE.NAME
	   END COL_TYPE
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
	 , FK.referenced_object
	 , FK.referenced_column_name
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

    return sql