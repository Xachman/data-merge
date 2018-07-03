import unittest
import os
import pymysql.cursors


class TestMerge(unittest.TestCase):
    def setUp(self):
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
            `key` varchar(255) COLLATE utf8_bin NOT NULL,
            `value` varchar(255) COLLATE utf8_bin NOT NULL,
            PRIMARY KEY(`id`),
            FOREIGN KEY (user_id) REFERENCES users(id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1;
        """

        insertDB1 = """
        INSERT INTO users (username, email) VALUES ('timtheone', 'tim@timetheone.com'),
        ('sassysara', 'sara@gmail.com'),
        ('jason', 'jason@gmail.com');
        INSERT INTO users_meta (user_id, key, value) VALUES (1, 'address', '123 tim lane'),
        (2, 'address', '567 main street'),
        (2, 'phone', '555-555-5556'),
        (3, 'address', '935 wall street'),
        (3, 'phone', '555-555-5589');
        """
        insertDB2 = """
        INSERT INTO users (username, email) VALUES ('timtheone', 'tim@timetheone.com'),
        ('sassysara', 'sara@gmail.com');
        INSERT INTO users (username, email) VALUES ('john', 'john@gmail.com');
        INSERT INTO users_meta (user_id, key, value) VALUES (1, 'address', '123 tim lane');
        INSERT INTO users_meta (user_id, key, value) VALUES (2, 'address', '567 main street');
        INSERT INTO users_meta (user_id, key, value) VALUES (3, 'address', '456 elm street');
        INSERT INTO users_meta (user_id, key, value) VALUES (2, 'phone', '555-555-5557');
        INSERT INTO users_meta (user_id, key, value) VALUES (3, 'phone', '555-555-5550');
        """
        try:
            print("db1 commit")
            with db1.cursor() as cursor:
                cursor.execute(usersTableSql)
                cursor.execute(usersMetaTableSql)
                cursor.execute(insertDB1)
                db1.commit()
            with db2.cursor() as cursor:
                cursor.execute(usersTableSql)
                cursor.execute(usersMetaTableSql)
                cursor.execute(insertDB2)
                db2.commit()
        finally:
            db1.close()
            db2.close()
        
    def test_merge_simple(self):
        print("testmerge")
        self.assertTrue(False)
