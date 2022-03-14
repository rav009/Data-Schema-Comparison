# -*- coding:utf-8 -*-

import teradatasql as td
import configparser


class teradataDefinition:
    def __init__(self, configpath, section, db):
        self.init_ini_filename = configpath
        self.config = configparser.ConfigParser()
        self.db = db
        try:
            self.config.read_file(open(self.init_ini_filename, encoding='UTF-8'))
            self.ini_section_name = section
            self.ini_username = self.config.get(self.ini_section_name, 'database_user')
            self.ini_password = self.config.get(self.ini_section_name, 'database_pwd')
            self.ini_host = self.config.get(self.ini_section_name, 'server_host')
            self.ini_teradata_mode = self.config.get(self.ini_section_name, 'teradata_mode')

        except Exception as exc:
            print('read ini file error, init TD CONNECTION failed!')

        # init teradata connection
        try:
            self.teradata_connection = td.connect(host=self.ini_host,
                                                  user=self.ini_username,
                                                  password=self.ini_password,
                                                  tmode=self.ini_teradata_mode)
            self.teradata_cur = self.teradata_connection.cursor()
        except Exception as exc:
            print('Teradata connection init failed')
            return None
            
    def list_ObjectSQL(self):
        return  """SELECT UPPER(DATABASE_NAME) AS DB_NAME,UPPER(TVM_NAME) AS TVM_NAME,UPPER(TVM_KIND) AS
                   TVM_KIND FROM MTDT_PUB.V_TVM WHERE UPPER(DATABASE_NAME) LIKE '%s'""" % self.db

    def list_ChangedObjectSQL(self, lastalterdt):
        return  """SELECT UPPER(DATABASE_NAME) AS DB_NAME,UPPER(TVM_NAME) AS TVM_NAME,UPPER(TVM_KIND) AS
                   TVM_KIND FROM MTDT_PUB.V_TVM WHERE UPPER(DATABASE_NAME) LIKE '%s'
                   and  last_alter_timestamp > to_timestamp('%s','yyyy-mm-dd hh24:mi:ss')""" % (self.db, lastalterdt)

    def list_Object(self):
        return self.query(self.list_ObjectSQL())

    def list_ChangedObject(self, lastalterdt):
        return self.query(self.list_ChangedObjectSQL(lastalterdt))

    def query(self, sql):
        if self.teradata_cur:
            try:
                self.teradata_cur.execute(sql)
                return self.teradata_cur.fetchall()
            except Exception as exc:
                print('exec query error:')
                print(sql)
        else:
            return None
            
    def queryone(self, sql):
        if self.teradata_cur:
            try:
                self.teradata_cur.execute(sql)
                return self.teradata_cur.fetchone()
            except Exception as exc:
                print('exec query error:')
                print(sql)
        else:
            return None

    def list_showsql(self, obj_list):
        rs = []
        for o in obj_list:
            schema = o[0]
            obj_name = o[1]
            obj_type = o[2]
            def_sql = ''
            if obj_type == 'T' or obj_type == 'O':
                rs.append((schema, obj_name, obj_type, 'show table ' + schema + '.' + obj_name))
            elif obj_type == 'V':
                rs.append((schema, obj_name, obj_type, 'show view ' + schema + '.' + obj_name))
            elif obj_type == 'P' and 'SP_DATA_SEPARATION' not in obj_name:
                rs.append((schema, obj_name, obj_type, 'show procedure ' + schema + '.' + obj_name))
        return rs
        
    def list_createsql(self, showlist):
        rs = []
        for schema, name, obj_type, showsql in showlist:
            csqls = self.query(showsql)
            csql = ''.join([s[0] for s in csqls]).replace("\r", "\n")
            rs.append((schema, name, obj_type, csql))
        return rs
            
    def list_full_schema(self):
        objs = self.list_Object()
        showsqls = self.list_showsql(objs)
        createsqls = self.list_createsql(showsqls)
        return createsqls

    def list_changed_schema(self, lastalterdt):
        objs = self.list_ChangedObject(lastalterdt)
        showsqls = self.list_showsql(objs)
        createsqls = self.list_createsql(showsqls)
        return createsqls
            
    def close(self):
        if self.teradata_cur:
            self.teradata_cur.close()
        if self.teradata_connection:
            self.teradata_connection.close()


if __name__ == '__main__':
    print("test")
    td_def = teradataDefinition("./Config.ini", "a", "b")
    showsqls = td_def.list_changed_schema('2022-01-30 00:00:00')
    print(showsqls)

    td_def.close()
