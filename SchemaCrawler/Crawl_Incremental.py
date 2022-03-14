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
        'NCAPCNNGBI_CDI_DM',
        'NCAPCNNGBI_CDI_DM_ETL',
        'NCAPCNNGBI_AMPLUS_ETL',
        'NCAPCNNGBI_AMPLUS_PUB',
        'NCAPCNNGBI_CDI_DM_PUB',
        'NCAPCNNGBI_CDI_DM_RPT',
        'NCAPCNNGBI_CUST_ETL',
        'NCAPCNNGBI_CUST_PUB',
        'NCAPCNNGBI_PROD_ETL',
        'NCAPCNNGBI_PROD_PUB',
        'NCAPCNNGBI_SALES_ETL',
        'NCAPCNNGBI_SALES_PUB',
        'NCAPCNNGBI_AMPLUS',
        'NCAPCNNGBI_CUST',
        'NCAPCNNGBI_PROD',
        'NCAPCNNGBI_SALES',
        'NCAPCNNGBI_DM',
        'NCAPCNNGBI_DM_ETL',
        'NCAPCNNGBI_DM_PUB',
        'NCAPCNNGBI_DM_RPT',
        'NCAPCNNGBI_EMPL',
        'NCAPCNNGBI_EMPL_ETL',
        'NCAPCNNGBI_EMPL_PUB',
        'NCAPCNNGBI_CALL',
        'NCAPCNNGBI_CALL_ETL',
        'NCAPCNNGBI_CALL_PUB',
        'NCAPCNNGBI_SALES_RPT',
        'NCAPCNNGBI_MCM',
        'NCAPCNNGBI_MCM_ETL',
        'NCAPCNNGBI_MCM_PUB',
        'NCAPCNNGBI_MEDED',
        'NCAPCNNGBI_MEDED_ETL',
        'NCAPCNNGBI_MEDED_PUB',
        'NCAPCNNGBI_MCM_RPT',
        'NCAPCNNGBI_OTHER',
        'NCAPCNNGBI_OTHER_ETL',
        'NCAPCNNGBI_OTHER_PUB',
        'NCAPCNNGBI_DT',
        'NCAPCNNGBI_DT_ETL',
        'NCAPCNNGBI_DT_PUB',
        'NCAPCNNGBI_DM_WORK',
        'NCAPCNNGBI_AMPLUS_WORK',
        'NCAPCNNGBI_CALL_WORK',
        'NCAPCNNGBI_CUST_WORK',
        'NCAPCNNGBI_MCM_WORK',
        'NCAPCNNGBI_MEDED_WORK',
        'NCAPCNNGBI_OTHER_WORK',
        'NCAPCNNGBI_PROD_WORK',
        'NCAPCNNGBI_SALES_WORK',
        'NCAPCNNGBI_CNTRL',
        'NCAPCNNGBI_CNTRL_ETL',
        'NCAPCNNGBI_FIN',
        'NCAPCNNGBI_FIN_ETL',
        'NCAPCNNGBI_HR',
        'NCAPCNNGBI_HR_ETL',
        'NCAPCNNGBI_HR_PUB',
        'NCAPCNNGBI_HR_WORK',
        'NCAPCNNGBI_SEC',
        'NCAPCNNGBI_SEC_ETL',
        'NCAPCNNGBI_SEC_PUB'
    ]
    for t in Teradata_list:
        Teradata_Incremental(t, ini_path)
