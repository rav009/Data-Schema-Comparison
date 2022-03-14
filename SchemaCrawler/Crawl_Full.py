import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))

from Database import SqliteDAO
import Teradata
import logging
logging.basicConfig(level=logging.NOTSET)  # 设置日志级别


if __name__ == "__main__":
    ini_path = os.path.dirname(os.path.abspath(__file__)) + os.path.sep + "Config.ini"

    dbname = input("Please input the teradata database name (Enter to Skip):")
    if dbname and dbname!='':
        logging.info("Begin to load the full objects schema from %s" % dbname)
        NCAPCNNGBI_CDI_DM = Teradata.teradataDefinition(ini_path, "Teradata_Prod", dbname)
        sl = NCAPCNNGBI_CDI_DM.list_full_schema()
        logging.info("%d Objects loaded from %s" % (len(sl), dbname))
        NCAPCNNGBI_CDI_DM.close()
        SqliteDAO.InsertSchema(sl)
        logging.info("Insert Complete")
