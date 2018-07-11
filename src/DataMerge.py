import pymysql.cursors
from .Database import Database
class DataMerge:
    def __init__(self, db1: Database, db2: Database):
        self.db1 = db1
        self.db2 = db2
        self.forginKeys = []

    def merge(self):
        # merge data
        self.findRelations(self.db1) 
        self.diffDatabases(self.db1, self.db2)
        print(self.forginKeys)
        return False

    def connect(self, db: Database):
        if db.database != '':
            return pymysql.connect(host=db.host,
                                user=db.user,
                                password=db.password,
                                database=db.database,
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)
        else:
            return pymysql.connect(host=db.host,
                                user=db.user,
                                password=db.password,
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)

    def findRelations(self, db: Database):
        dbc = self.connect(db)
        try:
            with dbc.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                results = cursor.fetchall()
                for result in results:
                    keys = self.getForginKey(db, result["Tables_in_"+db.database])
                    if len(keys) > 0:
                        for key in keys:
                            self.forginKeys.append(key)
        finally:
            dbc.close()
        
    def getForginKey(self, db: Database, table):

        query = """
            SELECT
                TABLE_NAME,
                COLUMN_NAME,
                CONSTRAINT_NAME,
                REFERENCED_TABLE_NAME,
                REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE
                REFERENCED_TABLE_SCHEMA = '{database}' AND
                REFERENCED_TABLE_NAME = '{table}';
            """.format(table=table, database=db.database)
        dbc = self.connect(Database(db.host, db.user, db.password, ''))
        try:
            with dbc.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                if len(results) > 0:
                    forginKeys = []
                    for result in results:
                        forginKeys.append({"table": result["TABLE_NAME"], "forgin_table": result["REFERENCED_TABLE_NAME"], "column": result["COLUMN_NAME"]})
                    return forginKeys
        finally:
            dbc.close()
        return []

    def diffDatabases(self, db1: Database, db2: Database):
        dbc1 = self.connect(db1) 
        dbc2 = self.connect(db2)

        try:
            with dbc1.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                results = cursor.fetchall()
                for result in results:
                    data1 = self.getTableData(result["Tables_in_"+db1.database], db1)
                    data2 = self.getTableData(result["Tables_in_"+db1.database], db2)
                    self.compareData(data1, data2)

        finally:
            dbc1.close()
    
    def getTableData(self, table, db: Database):
        dbc = self.connect(db)

        try:
            with dbc.cursor() as cursor:
                cursor.execute("SELECT * FROM {table}".format(table=table))
                results = cursor.fetchall()
                return results
        finally:
            dbc.close()

    def compareData(self, data1, data2):
        for i, val1 in enumerate(data1):
            val2 = data2[i]
            print(val2)



