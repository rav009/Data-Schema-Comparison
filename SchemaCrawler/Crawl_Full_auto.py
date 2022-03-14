import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))

from Database import SqliteDAO
import Teradata
import logging
logging.basicConfig(level=logging.NOTSET)  # 设置日志级别


if __name__ == "__main__":
    ini_path = os.path.dirname(os.path.abspath(__file__)) + os.path.sep + "Config.ini"
    # Database list:
    # NCAPCNNGBI_CDI_DM, NCAPCNNGBI_CDI_DM_ETL, NCAPCNNGBI_AMPLUS_ETL,NCAPCNNGBI_AMPLUS
    dblist = [
        "NCAPCNNGBI_MEDED",
        "NCAPCNNGBI_MEDED_ETL",
        "NCAPCNNGBI_MEDED_PUB",
        "NCAPCNNGBI_MCM_RPT",
        "NCAPCNNGBI_OTHER",
        "NCAPCNNGBI_OTHER_ETL",
        "NCAPCNNGBI_OTHER_PUB",
        "NCAPCNNGBI_DT",
        "NCAPCNNGBI_DT_ETL",
        "NCAPCNNGBI_DT_PUB",
        "NCAPCNNGBI_DM_WORK",
        "NCAPCNNGBI_AMPLUS_WORK",
        "NCAPCNNGBI_CALL_WORK",
        "NCAPCNNGBI_CUST_WORK",
        "NCAPCNNGBI_MCM_WORK",
        "NCAPCNNGBI_MEDED_WORK",
        "NCAPCNNGBI_OTHER_WORK",
        "NCAPCNNGBI_PROD_WORK",
        "NCAPCNNGBI_SALES_WORK",
        "NCAPCNNGBI_CNTRL",
        "NCAPCNNGBI_CNTRL_ETL",
        "NCAPCNNGBI_FIN",
        "NCAPCNNGBI_FIN_ETL",
        "NCAPCNNGBI_HR",
        "NCAPCNNGBI_HR_ETL",
        "NCAPCNNGBI_HR_PUB",
        "NCAPCNNGBI_HR_WORK",
        "NCAPCNNGBI_SEC",
        "NCAPCNNGBI_SEC_ETL",
        "NCAPCNNGBI_SEC_PUB"
    ]

    for dbname in dblist:
        if dbname and dbname!='':
            logging.info("Begin to load the full objects schema from %s" % dbname)
            NCAPCNNGBI_CDI_DM = Teradata.teradataDefinition(ini_path, "Teradata_Prod", dbname)
            sl = NCAPCNNGBI_CDI_DM.list_full_schema()
            logging.info("%d Objects loaded from %s" % (len(sl), dbname))
            NCAPCNNGBI_CDI_DM.close()
            SqliteDAO.InsertSchema(sl)
            logging.info("Insert Complete")