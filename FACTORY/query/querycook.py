def MakeColumnQueryStatement(dbms, columnInfoDatas):
    statementList = []
    for columnInfo in columnInfoDatas:
        columnId = columnInfo['COLUMN_ID']
        if columnInfo['BYTE_YN'] == 'Y':
            if dbms == 'MSSQL':
                columnValue = f"{columnId} = " + "'0x' + " + f"CONVERT(VARCHAR(MAX), CAST({columnId} AS VARBINARY(MAX)), 2)"
            elif dbms == 'POSTGRESQL':
                columnValue = f"{columnId} = " + "'\\x' || " + f"encode({columnId}, 'hex')" 
        else:
            columnValue = f"{columnId}"
        statementList.append(columnValue)        

    columnInfo = ', '.join(statementList)

    return columnInfo