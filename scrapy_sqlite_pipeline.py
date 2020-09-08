import json
import sqlite3
import time


class SqlitePipeline:
    CREATE_SQL = '''
CREATE TABLE IF NOT EXISTS {table} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER,
    job INTEGER NOT NULL,
    json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_{table}_timestamp
ON {table} (timestamp);
CREATE INDEX IF NOT EXISTS idx_{table}_job
ON {table} (job);
    '''
    INSERT_SQL = '''
INSERT INTO {table} (timestamp, job, json)
VALUES (:timestamp, :job, :json);
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
            item,
        )
        return item

    def _insert(self, table, job, data):
        timestamp = int(time.time())
        data_json = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
        self.cursor.execute(
            self.INSERT_SQL.format(table=table),
            dict(timestamp=timestamp, job=job, json=data_json),
        )

    def _create(self, table):
        self.cursor.executescript(self.CREATE_SQL.format(table=table))
