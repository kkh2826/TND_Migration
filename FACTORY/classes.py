import pymssql
import psycopg2
import psycopg2.extras


'''
    DB 연결 객체
'''
class DBInfo:
    def __init__(self, dbms, server, port, username, password, database):
        self.dbms = dbms
        self.server = server
        self.port = port
        self.username = username
        self.password = password
        self.database = database


'''
    DBMS 클래스
'''
class DBMS:
    def __init__(self, dbms, server, port, username, password, database):
        self.server = server
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.dbms = dbms
        self.connectionObject = None


    def Connect(self):
        dbms = self.dbms

        try:
            if dbms.upper() == 'MSSQL':
                self.connectionObject = pymssql.connect(server=self.server, port=self.port, user=self.username, password=self.password, database=self.database, login_timeout=20, timeout=20)
            elif dbms.upper() == 'POSTGRESQL':
                self.connectionObject = psycopg2.connect(host=self.server, port=self.port, user=self.username, password=self.password, dbname=self.database, connect_timeout=20)
        except:
            return None

        return self.connectionObject

    def ExecuteQuery(self, query):
        cursor = None
        dbms = self.dbms

        self.connectionObject = self.Connect()

        try:
            if dbms.upper() == 'MSSQL':
                cursor = self.connectionObject.cursor(as_dict=True)
            elif dbms.upper() == 'POSTGRESQL':
                cursor = self.connectionObject.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(query)
        except:
            return None

        data = cursor.fetchall()

        return data

    def Close(self):
        self.connectionObject.close()