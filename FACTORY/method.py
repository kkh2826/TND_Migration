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