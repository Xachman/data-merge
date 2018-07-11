import unittest
import os
import pymysql.cursors
from .DataMerge import DataMerge
from .Database import Database


class TestMerge(unittest.TestCase):
    def setUp(self):
        removeDB1 = """
        DROP DATABASE database1
        """
        removeDB2 = """
        DROP DATABASE database2
        """
        createDb1 = """
        CREATE DATABASE IF NOT EXISTS database1
        """
        createDb2 = """
        CREATE DATABASE IF NOT EXISTS database2
        """
        dbc = pymysql.connect(host='mysqlhost',
                             user='root',
                             password='root',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
        try:
            with dbc.cursor() as cursor:
                cursor.execute(removeDB1)
                cursor.execute(removeDB2)
                cursor.execute(createDb1)
                cursor.execute(createDb2)
                dbc.commit()
        finally:
            dbc.close()

        db1 = pymysql.connect(host='mysqlhost',
                             user='root',
                             password='root',
                             db='database1',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
        db2 = pymysql.connect(host='mysqlhost',
                             user='root',
                             password='root',
                             db='database2',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
        usersTableSql = """
        CREATE TABLE IF NOT EXISTS `users` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `username` varchar(255) COLLATE utf8_bin NOT NULL,
            `email` varchar(255) COLLATE utf8_bin NOT NULL,
            PRIMARY KEY(`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1;
        """
        usersMetaTableSql = """
        CREATE TABLE IF NOT EXISTS `users_meta` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `user_id` int(11) NOT NULL,
            `meta_key` varchar(255) COLLATE utf8_bin NOT NULL,
            `value` varchar(255) COLLATE utf8_bin NOT NULL,
            PRIMARY KEY(`id`),
            FOREIGN KEY (user_id) REFERENCES users(id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1;
        """

        insertDB1 = """
        INSERT INTO users (username, email) VALUES ('timtheone', 'tim@timetheone.com'),
        ('sassysara', 'sara@gmail.com'),
        ('jason', 'jason@gmail.com');
        """
        insertDB1_2 = """
        INSERT INTO users_meta (user_id, meta_key, value) VALUES (1, 'address', '123 tim lane'),
        (2, 'address', '567 main street'),
        (2, 'phone', '555-555-5556'),
        (3, 'address', '935 wall street'),
        (3, 'phone', '555-555-5589');
        """

        insertDB2 = """
        INSERT INTO users (username, email) VALUES ('timtheone', 'tim@timetheone.com'),
        ('john', 'john@gmail.com'),
        ('sassysara', 'sara@gmail.com');
        """
        insertDB2_2 = """
        INSERT INTO users_meta (user_id, meta_key, value) VALUES 
        (1, 'address', '123 tim lane'),
        (2, 'address', '567 main street'),
        (3, 'address', '456 elm street'),
        (2, 'phone', '555-555-5557'),
        (3, 'phone', '555-555-5550');
        """
        try:
            print("db1 commit")
            with db1.cursor() as cursor:
                cursor.execute(usersTableSql)
                cursor.execute(usersMetaTableSql)
                cursor.execute(insertDB1)
                cursor.execute(insertDB1_2)
                db1.commit()
            with db2.cursor() as cursor:
                cursor.execute(usersTableSql)
                cursor.execute(usersMetaTableSql)
                cursor.execute(insertDB2)
                cursor.execute(insertDB2_2)
                db2.commit()
        finally:
            db1.close()
            db2.close()
    def getDB2(self):
        return pymysql.connect(host='mysqlhost',
                             user='root',
                             password='root',
                             db='database2',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
    def test_merge_simple(self):
        dm = DataMerge(Database('mysqlhost','root','root','database1'),
        Database('mysqlhost','root','root','database2'))
        dm.merge()
        db2 = self.getDB2()
        try:
            with db2.cursor() as cursor:
                cursor.execute("SELECT * FROM users")
                result = cursor.fetchall()
                self.assertTrue(result[3]["username"], "jason")
        finally:
            db2.close()