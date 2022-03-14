import sqlite3
import os
import datetime
from urllib.parse import quote

def getConn():
    pwd = os.path.dirname(__file__)
    db_path = pwd + os.path.sep + "Data_Seal.db"
    db = sqlite3.connect(db_path)
    return db

def getNowStr():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def getMaxLogDT(dbname):
    db = getConn()
    sql = "select max(crawl_time) from log where db_name like '%s'" % dbname
    cur = db.cursor()
    cur.execute(sql)
    rs = cur.fetchone()[0]
    cur.close()
    db.close()
    return rs


def InsertSchema(schemalist, crawl_type="full"):
    db = getConn()
    start_time = getNowStr()
    tpl = "insert into ddl select null,'%s','%s','%s','%s','%s',''"
    schema_name = None
    for schema, name, obj_type, csql in schemalist:
        sql = tpl % (schema, name, obj_type, quote(csql), start_time)
        db.execute(sql)
        if not schema_name:
            schema_name = schema
    db.execute("insert into log select null, '%s', '%s', '%s'" % (schema_name, crawl_type, start_time))
    db.commit()
    db.close()


def UpdateComment(id, comment):
    db = getConn()
    sql = "update ddl set comment='%s' where id=%d" % (str(comment).replace("'","''"), id)
    db.execute(sql)
    db.commit()
    db.close()


def getChangedDDL(startdt, notlike):
    '''
    :param startdt: "yyyy-mm-dd HH:mm:ss"
    :return: [[db_name, obj_name, obj_type, d1time, d1ddl, d2ddl, id, comment],...]
    '''
    ns = ''.join([" and obj_name not like '" + str(s) + "' "  for s in  notlike.split(',')])
    sql = """Select db_name, obj_name, obj_type, d1time, d1ddl, d2ddl, id, comment from (
SELECT
 id
,db_name
,obj_name
,obj_type
,d1time
,d1ddl
,d2ddl
,comment
, rank() over (partition by db_name,obj_name,obj_type,d1time order by d2time)=1 as RK
FROM
(
SELECT 
 d.id
,d.db_name
,d.obj_name
,d.obj_type
,d.obj_time as d1time
,d2.obj_time AS d2time
,d.obj_ddl as d1ddl
,d2.obj_ddl as d2ddl
,d.comment
FROM ddl d
JOIN log l on d.obj_time = l.crawl_time  and l.crawl_type like 'Incremental'
left join ddl d2 on d.db_name=d2.db_name and d.obj_name=d2.obj_name and d.obj_type=d2.obj_type and d.obj_time>d2.obj_time
where d.obj_time > "%s"
) t
) t2
where RK=1 
%s
and ifnull(d2ddl,'')<>ifnull(d1ddl,'')
order by d1time desc
limit 150 """ % (startdt, ns)
    db = getConn()
    cur = db.cursor()
    cur.execute(sql)
    rs = cur.fetchall()
    cur.close()
    db.close()
    return rs