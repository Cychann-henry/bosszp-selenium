#!/usr/bin/python
# -*- coding:utf-8 -*-
"""定义 DBUtils 工具类, 封装 PostgreSQL 常用操作 API - 方法1(select_all): 查询所有数据
    - 方法2(select_one): 查询一条数据
    - 方法3(select_n): 查询前 n 条数据
    - 方法4(insert_data): 插入数据
    - 方法5(update_data): 更新数据
    - 方法6(delete_data): 删除数据
"""
import psycopg2
import psycopg2.extras


class DBUtils:
    def __init__(self, host, user, password, db, port=5432):
        """
        DBUtils 初始化方法，实例化 DBUtils 类的时候会默认调用,仅调用 1 次
        """
        self.conn = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            dbname=db,
            port=port,
        )
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def select_all(self, sql, args=None):
        """
        查询所有数据
        :param sql: 查询数据的 sql（使用 %s 占位符）
        :param args: 参数，只能是元组或者列表
        :return: 返回结果集
        """
        self.cursor.execute(sql, args)
        result = self.cursor.fetchall()
        return result

    def select_n(self, sql, n, args=None):
        """
        查询满足条件的前 n 条数据
        """
        self.cursor.execute(sql, args)
        result = self.cursor.fetchmany(n)
        return result

    def select_one(self, sql, args=None):
        """
        查询满足条件的第 1 条数据
        """
        self.cursor.execute(sql, args)
        result = self.cursor.fetchone()
        return result

    def insert_data(self, sql, args=None):
        """
        插入数据
        """
        self.cursor.execute(sql, args)
        self.conn.commit()
        return self.cursor.rowcount

    def update_data(self, sql, args=None):
        """
        更新数据
        """
        self.cursor.execute(sql, args)
        self.conn.commit()
        return self.cursor.rowcount

    def delete_data(self, sql, args=None):
        """
        删除数据
        """
        self.cursor.execute(sql, args)
        self.conn.commit()
        return self.cursor.rowcount

    def close(self):
        """
        关闭数据库连接
        """
        self.cursor.close()
        self.conn.close()


if __name__ == "__main__":
    # 示例：请按本地环境修改连接参数
    db = DBUtils("localhost", "postgres", "your_password", "spider_db")
    db.close()
