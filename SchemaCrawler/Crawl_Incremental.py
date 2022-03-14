import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))

from Database import SqliteDAO
import Teradata
import logging
logging.basicConfig(level=logging.NOTSET)  # 设置日志级别

def Teradata_Incremental(dbname, ini_path):
    lastSyncDT = SqliteDAO.getMaxLogDT(dbname)
    logging.info("The last sync datetime of %s is %s" % (dbname, lastSyncDT))
    td = Teradata.teradataDefinition(ini_path, "Teradata_Prod", dbname)
    sl = td.list_changed_schema(lastSyncDT)
    td.close()
    if len(sl) > 0:
        logging.info("%d of changed objects in %s detected" % (len(sl), dbname))
        SqliteDAO.InsertSchema(sl, crawl_type="Incremental")
    else:
        logging.info("0 changed objects in %s detected" % dbname)


if __name__ == "__main__":
    ini_path = os.path.dirname(os.path.abspath(__file__)) + os.path.sep + "Config.ini"
    logging.info("ini_path: %s" % ini_path)
    # Teradata
    Teradata_list = [
        'a.b'
    ]
    for t in Teradata_list:
        Teradata_Incremental(t, ini_path)
