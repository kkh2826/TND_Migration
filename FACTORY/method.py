from FACTORY.classes import DBInfo

def GetDBInfo(self):
    dbms = self.data['dbms']
    server = self.data['server']
    port = self.data['port']
    username = self.data['username']
    password = self.data['password']
    database = self.data['database']

    dbInfo = DBInfo(dbms, server, port, username, password, database)

    return dbInfo


def MakeColumnQueryStatement(columnInfoDatas):
    statementList = []
    for columnInfo in columnInfoDatas:
        columnId = columnInfo['COLUMN_ID']
        if columnInfo['BYTE_YN'] == 'Y':
            columnValue = f"{columnId} = " + "'0x' + " + f"CONVERT(VARCHAR(MAX), CAST({columnId} AS VARBINARY(MAX)), 2)"
        else:
            columnValue = f"{columnId}"
        statementList.append(columnValue)        

    columnInfo = ', '.join(statementList)

    return columnInfo