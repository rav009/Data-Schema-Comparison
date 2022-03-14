import os
import sys
sys.path.append(os.path.dirname(__file__))

from Database import SqliteDAO

db = SqliteDAO.getConn()
db.execute("""drop table if exists ddl;""")
db.execute("""drop table if exists log;""")
db.commit()

db.execute("""create table if not exists ddl (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    db_name TEXT,
    obj_name TEXT,
    obj_type TEXT,
    obj_ddl TEXT,
    obj_time TEXT，
    comment TEXT default ''
);""")
db.execute("CREATE INDEX ddl_idx ON ddl (db_name,obj_name,obj_type);")
db.commit()

db.execute("""create table if not exists log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    db_name TEXT,
    crawl_type TEXT,
    crawl_time TEXT
);""")
db.commit()

cur = db.cursor()
rs = cur.execute("select * from ddl")

for row in rs:
    print(row)

db.close()