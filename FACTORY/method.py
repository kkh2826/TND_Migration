from FACTORY.classes import DBInfo

from FACTORY.query.mssql import GetColumnInfoDataQuery_MSSQL, GetDBBasicInfoDataQuery_MSSQL, GetSampleDataQuery_MSSQL
from FACTORY.query.postgresql import GetDBBasicInfoDataQuery_POSTGRESQL, GetSampleDataQuery_POSTGRESQL, GetColumnInfoDataQuery_POSTGRESQL


def GetDBInfo(self):
    dbms = self.data['dbms']
    server = self.data['server']
    port = self.data['port']
    username = self.data['username']
    password = self.data['password']
    database = self.data['database']

    dbInfo = DBInfo(dbms, server, port, username, password, database)

    return dbInfo


def get_db_basicinfo_data_query(dbms):
    if dbms.upper() == 'MSSQL':
        return GetDBBasicInfoDataQuery_MSSQL()
    elif dbms.upper() == 'POSTGRESQL':
        return GetDBBasicInfoDataQuery_POSTGRESQL()


def get_columninfo_data_query(dbms, schema, table):
    if dbms.upper() == 'MSSQL':
        return GetColumnInfoDataQuery_MSSQL(schema, table)
    elif dbms.upper() == 'POSTGRESQL':
        return GetColumnInfoDataQuery_POSTGRESQL(schema, table)


def get_sample_data_query(dbms, schema, table, columnInfoDatas):
    if dbms.upper() == 'MSSQL':
        return GetSampleDataQuery_MSSQL(schema, table, columnInfoDatas)
    elif dbms.upper() == 'POSTGRESQL':
        return GetSampleDataQuery_POSTGRESQL(schema, table, columnInfoDatas)
