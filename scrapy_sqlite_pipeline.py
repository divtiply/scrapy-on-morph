import json
import sqlite3
import textwrap
import time


class SqlitePipeline:
    CREATE_TABLE_SQL = '''
CREATE TABLE IF NOT EXISTS %s (
    timestamp INTEGER PRIMARY KEY,
    job INTEGER NOT NULL,
    data TEXT NOT NULL
);
    '''
    CREATE_INDEX_SQL = '''
CREATE INDEX IF NOT EXISTS idx_%s_job
ON %s (job);
    '''
    INSERT_SQL = '''
INSERT INTO %s (timestamp, job, data)
VALUES (?,?,?);
    '''

    def __init__(self, database='data.sqlite', table=None, job=None):
        self.database = database
        self.table = table
        self.job = job or int(time.time())

    def open_spider(self, spider):
        self.connection = sqlite3.connect(self.database)
        self.cursor = self.connection.cursor()
        self._create(self.table or spider.name)

    def close_spider(self, spider):
        self.connection.commit()
        self.connection.close()

    def process_item(self, item, spider):
        self._insert(
            self.table or spider.name,
            self.job,
            json.dumps(item, ensure_ascii=False, separators=(',', ':')),
        )
        return item

    def _insert(self, table, job, data):
        timestamp = int(time.time() * 1e6)
        self.cursor.execute(self.INSERT_SQL % table, (timestamp, job, data))

    def _create(self, table):
        self.cursor.execute(self.CREATE_TABLE_SQL % table)
        self.cursor.execute(self.CREATE_INDEX_SQL % ((table,) * 2))
