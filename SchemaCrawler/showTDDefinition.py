# -*- coding:utf-8 -*-
# !/usr/bin/env python
# @author:Herman Chen
# @Description: export TD objects VIEW & SP creation scripts.

"""
Export TD objects VIEW & SP creation scripts.
"""

import teradatasql as td
import configparser
import os
from pathlib import Path


class teradataDefinition:
    def __init__(self):
        self.init_ini_filename = 'Config.ini'
        self.config = configparser.ConfigParser()
        try:
            self.config.read_file(open(self.init_ini_filename, encoding='UTF-8'))
            self.ini_section_name = 'TD_CONFIG'
            self.ini_username = self.config.get(self.ini_section_name, 'database_user')
            self.ini_password = self.config.get(self.ini_section_name, 'database_pwd')
            self.ini_host = self.config.get(self.ini_section_name, 'server_host')
            self.ini_teradata_mode = self.config.get(self.ini_section_name, 'teradata_mode')
            self.ini_show_def_sql = """SELECT UPPER(DATABASE_NAME) AS DB_NAME,UPPER(TVM_NAME) AS TVM_NAME,UPPER(TVM_KIND) AS
                                    TVM_KIND FROM MTDT_PUB.V_TVM WHERE UPPER(DATABASE_NAME) LIKE 'NCAPCNNGBI%'"""
            self.ini_definition_export_folder = self.config.get(self.ini_section_name, 'definition_export_folder')
            self.ini_definition_clean_folder = self.config.get(self.ini_section_name, 'definition_clean_folder')
            self.teradata_connection = ''
            self.teradata_cur = ''
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

    def td_query(self, sql):
        if self.teradata_cur != '':
            try:
                self.teradata_cur.execute(sql)
                return self.teradata_cur.fetchall()
            except Exception as exc:
                print('exec query error:')
                print(sql)
        else:
            return None

    def show_definition(self):

        result = self.td_query(self.ini_show_def_sql)
        view_count = 0
        sp_count = 0
        for object in result:
            schema = object[0]
            obj_name = object[1]
            obj_type = object[2]
            def_sql = ''
            if obj_type == 'T' or obj_type == 'O':

                pass
            elif obj_type == 'V':
                def_sql = 'show view ' + schema + '.' + obj_name
                result2 = self.td_query(def_sql)
                print('running:', def_sql)
                definition = result2[0][0]
                with open(self.ini_definition_export_folder + schema + '.' + obj_name + '.sql', mode='w',
                          encoding='UTF-8') as export1:
                    export1.write(definition)
                    export1.close()
                view_count += 1
                pass
            elif obj_type == 'P' and 'SP_DATA_SEPARATION' not in obj_name:
                def_sql = 'show procedure ' + schema + '.' + obj_name
                result2 = self.td_query(def_sql)
                print('running:', def_sql)
                definition = result2[0][0]
                with open(self.ini_definition_export_folder + schema + '.' + obj_name + '.sql', mode='w',
                          encoding='UTF-8') as export2:
                    export2.write(definition)
                    export2.close()
                sp_count += 1
                pass

        print('total view:' + view_count)
        print('total sp:' + sp_count)

    def clean_note(self):

        """
        读取 对象创建脚本文件，输入文件路径
        去除 脚本中 的注释信息
        以行方式读取每一行中的内容。判断这一行中的字符情况
            a.-- 注释, 需要将注释符及后面的信息剔除，剔除信息存入日志
            b. /* 注释，出现/* 需伴随 */结束，将/**/中的内容存入日志
        :return: (  String , SP Name/View Name
                    String , SQL creation script)
        """

        # 先从导出的目录遍历出所有的脚本列表
        definition_list = list()
        folder_path = Path(self.ini_definition_export_folder)
        if folder_path.exists():
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    print("path_output:" + str(os.path.join(root, file)))
                    definition_list.append([root, file])


        for file_path_info in definition_list:
            # 变量初始化
            sql = ''
            # 用来判单 次行是否要记作 注释段
            next_line_is_para_ann = 0
            row_count = 0
            # file_path_info[1] 文件名 ， file_path_info[0] 路径
            filepath = os.path.join(file_path_info[0], file_path_info[1])
            # 获取文件路径后，读取文件，逐行读取进行判断 并输出
            with open(filepath, 'r', encoding='utf-8-sig') as file_to_read:

                while True:
                    # 整行读取数据
                    lines = file_to_read.readline().upper()
                    # 替换 UNICODE 转化为可读代码
                    if '_UNICODE' in lines:
                        lines = lines.replace('_UNICODE', ' _UNICODE').replace('_UNICODE ', '_UNICODE'). \
                            replace('||', ' || ').replace('(', ' ( ').replace(')', ' ) ').replace(',', ' , ') \
                            .replace(';', ' ; ').replace('/*', ' /* ').replace('*/', ' */ ')
                        lines_un_list = lines.split(' ')
                        for i, item in enumerate(lines_un_list):
                            if '_UNICODE' in item and item.count("'") == 2:
                                # do replace from teradata
                                print('item:' + item)
                                self.teradata_cur.execute("sel " + item + ' as TS')
                                lines_un_list[i] = "'" + self.teradata_cur.fetchone()[0] + "'"

                        lines = ' '.join(lines_un_list)

                        row_count += 1
                    # 行读取出来后，一些必须处理的做预处理
                    lines = lines.replace(' .', '.').replace('. ', '.')
                    # 每一行查看 -- /* */的位置，从同时出现的case 进行判断
                    address_split_line_ann = lines.find('--')
                    address_split_para_ann_1 = lines.find('/*')
                    address_split_para_ann_2 = lines.find('*/')
                    if not lines:
                        break
                        pass
                    if address_split_line_ann >= 0 or address_split_para_ann_1 >= 0 or address_split_para_ann_2 >= 0:
                        # lines_clean_ann
                        # 判断 -- 还是 /* 谁在前,2者需同时存在 但  上一行必须没有/* 即无需跳行
                        if address_split_line_ann >= 0 and address_split_para_ann_1 >= 0 and next_line_is_para_ann == 0:
                            # 取小的进行分割，取前端，后端存日志
                            if address_split_line_ann < address_split_para_ann_1:
                                sql += lines.split('--', 1)[0]
                                print('original lines:' + lines + '-- content :' + lines.split('--', 1)[1])
                            else:
                                sql += lines.split('/*', 1)[0]
                                print('original lines:' + lines + '/* content:' + lines.split('/*', 1)[1])
                                next_line_is_para_ann = 1
                        elif address_split_para_ann_1 >= 0 and address_split_para_ann_2 >= 0:
                            # /* & */ 同时存在，需要 一行解出中间段
                            lines_ann_3part = []
                            lines_in_firstpart = lines.split("/*", 1)
                            lines_ann_3part.append(lines_in_firstpart[0])
                            lines_ann_3part.extend(lines_in_firstpart[1].split('*/', 1))
                            # 取两侧
                            sql += lines_ann_3part[0]
                            sql += lines_ann_3part[2]

                        elif 0 <= address_split_para_ann_2 < address_split_line_ann and address_split_line_ann >= 0:
                            # */ xxx -- BBB 的情况 排除 */-- 连续的情况
                            if len(lines.split('*/--', 1)) <= 1:
                                lines_ann_3part = []
                                lines_in_firstpart = lines.split('*/', 1)
                                lines_ann_3part.append(lines_in_firstpart[0])
                                lines_ann_3part.extend(lines_in_firstpart[1].split('--', 1))
                                # 取中间段
                                sql += lines_ann_3part[1]
                            else:
                                print('original lines:' + lines + '*/-- content:' + lines)
                        # 但  上一行必须没有/* 即无需跳行
                        elif address_split_line_ann >= 0 and next_line_is_para_ann == 0:
                            sql += lines.split('--', 1)[0]
                            print('original lines:' + lines + '-- content:' + lines.split('--', 1)[-1])
                        # 但  上一行必须没有/* 即无需跳行
                        elif address_split_para_ann_1 >= 0 and next_line_is_para_ann == 0:
                            sql += lines.split('/*', 1)[0]
                            next_line_is_para_ann = 1
                            print('original lines:' + lines + '/* content:' + lines.split('/*', 1)[-1])
                        # 上一行必须没有/*的跳行标记
                        elif address_split_para_ann_2 >= 0 and next_line_is_para_ann == 1:
                            sql += lines.split('*/', 1)[-1]
                            print('original lines:' + lines + '*/ content:' + lines.split('*/', 1)[0])

                        if address_split_para_ann_2 >= 0:
                            # 跳过次行的逻辑判断符
                            next_line_is_para_ann = 0
                        sql += "\n"
                        pass
                    else:
                        # 无注释符
                        # 如果 次行跳过判断符为1 则直接计入日志，不拼接
                        if next_line_is_para_ann == 1:
                            print('original lines:' + lines)
                            sql += '\n'
                            continue
                        else:
                            sql += lines
            with open(self.ini_definition_clean_folder + file_path_info[1], mode='w', encoding='UTF-8') as out_sql:
                out_sql.write(sql)
                out_sql.close()



if __name__ == '__main__':
    td_def = teradataDefinition()
    #td_def.show_definition()
    td_def.clean_note()