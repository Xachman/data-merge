import pymysql.cursors
from .Database import Database
class DataMerge:
    def __init__(self, db1: Database, db2: Database):
        self.db1 = db1
        self.db2 = db2

    def merge(self):
        # merge data
        self.findRelations(self.db1) 
        return False

    def connect(self, db: Database):
        if db.database != '':
            print(db.database)
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
                forginKeys = []
                for result in results:
                    forginKeys.append(self.getForginKey(db, result["Tables_in_"+db.database]))
                print(results)
        finally:
            dbc.close()
        
    def getForginKey(self, db: Database, table):
        query = """
            SELECT
                TABLE_NAME,
                COLUMN_NAME,
                CONSTRAINT_NAME,   -- <<-- the one you want! 
                REFERENCED_TABLE_NAME,
                REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE
                REFERENCED_TABLE_SCHEMA = '{database}'
                REFERENCED_TABLE_NAME = '{table}';
            """.format(table=table, database=db.database)
        dbc = self.connect(Database(db.host, db.user, db.password, ''))
        try:
            with dbc.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                print(query)
                print(results)
        finally:
            dbc.close()
