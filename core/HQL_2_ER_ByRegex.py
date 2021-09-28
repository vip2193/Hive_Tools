#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os, sys
import re
from graphviz import Digraph, Graph

# 配置最多能显示的字段数量
MAX_NUM_OF_COLUMS = 18
TARGET_PATH = './target'

# 脚本使用方法
# 1.以文件夹作为参数 , 第二个参数为保存路径（默认为./target）  python HQL_ER_END.py   ../to_path  /save_path
# 2.使用hql文件作为参数 第二个参数为保存路径（默认为./target） python HQL_ER_END.py   ./test.hql  /save_path

# 字段去重
def columns_dunplicate(columns,column):
    new_col = []
    for col in columns:
        judge_str = re.findall(r'(\w+)\s*',col,re.S|re.I)
        if judge_str:
            judge_str = re.findall(r'(\w+)\s*',col,re.S|re.I)[0]
        else:
            continue
        if column != judge_str:
            index = re.search(r'\s+as\s+', col, re.I)
            if (index):
                new_col.append(col[index.span()[1]:])
            else:
                new_col.append(col)

    return new_col

def hql_file_to_ER(file):
    # 读取文件
    with open(file,encoding='utf-8') as f:
        hql = f.read()

        s = Digraph('structs', filename='structs_revisited.gv',
                    node_attr={'shape': 'record'},format='jpg')

        # 字符串转HTML表格结构
        def str2table_str(columns):
            # 限制字段个数
            const_columns = columns[0:MAX_NUM_OF_COLUMS]
            return ['<TR><TD COLSPAN="3">{}</TD></TR>'.format(column) for column in const_columns]

        #  主键字段HTML
        def pk2table_str(PK):
            # 限制字段个数
            return '<TR> <TD COLSPAN = "3" align = "CENTER"><B> PK： {} </B></TD> </TR>'.format(PK)


        # 通过表结构渲染表格
        def render_table(table_alias, table_name, table_str):
            if (table_alias):
                table_name = table_name + '({})'.format(table_alias)
            else:
                table_alias = table_name
            s.node(table_alias, fontname="Microsoft YaHei", label='''<
            <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
            <TR><TD COLSPAN="3"><B>{}</B></TD></TR>
              {}
            </TABLE>>'''.format(table_name, "\n".join(table_str)))
        # 构建关系连线
        def table_edge(table, target_table, label, arrowhead='None'):
            s.edge(table, target_table, label=label, arrowhead=arrowhead)

        # ------ 处理目标表

        target_t = re.findall(r'CREATE.*?TABLE.*?([\w\.]+?)`?\s*?\((.*?)\)\s*?COMMENT\s+', hql, re.S | re.M | re.I)[0]
        target_tname = target_t[0]
        target_t_columns = target_t[1]

        target_t_columns.replace('\n', '')
        target_t_columns = re.split(r',\s*?', target_t_columns)
        # 转换
        target_t_columns = [re.sub(r'[\n|\'|`]', '', colum) for colum in target_t_columns]
        print(target_tname, target_t_columns)
        target_pk = re.findall(r'--\s*?@\s*?Primary Key[：:]\s*?(\w+)\s*?', hql, re.S | re.M | re.I)[0]
        # 去重
        target_t_columns = columns_dunplicate(target_t_columns,target_pk)
        # 绘制目标表
        pk_str = pk2table_str(target_pk)
        # s = Graph('ER', filename='er.gv', engine='neato')
        #main_node_str = [target_pk].extend(str2table_str(target_t_columns))
        main_node_str = []
        main_node_str.append(pk_str)
        main_node_str.extend(str2table_str(target_t_columns))

        render_table('', target_tname, main_node_str)

        # -----  处理目标表结束

        # ----- 处理主表数据
        # 拿到数据的sql
        subtable = re.findall(r'insert.*?select(.*);', hql, re.S)
        # 主表字段
        main_table = re.findall(r'.*?from\s*?\(\s*?select\s*?(.*?)FROM\s+?([\w\.]+).*?\)\s*?(\w+)',subtable[0],re.S|re.I)
        if len(main_table) == 0:
            main_table = re.findall(r'\s*?(.*?)FROM\s+?([\w\.]+).*?',subtable[0],re.S|re.I)
        main_columns = main_table[0][0]  # 取第一个分区
        main_columns = main_columns.split('\n')
        new_columns = []
        for column in main_columns:
            index = re.search(r'\s+as\s+',column,re.I)
            if(index):
                column = column[index.span()[1]:]
                print(column)
            new_columns.append(column)

        main_columns = [re.sub(r'[\s|\n|\'|`,]','',colum) for colum in new_columns ] # 清洗
        main_tname = main_table[0][1]      # 取表名
        main_alias_name = 't1'
        
        main_pk = \
        re.findall(r'.*?from\s*?\(\s*?select.*?FROM.*?--\s*?mini_size:\s*?([\w, ]+?)\s', subtable[0], re.S | re.I)[
            0].replace(' ', '')

        print(main_columns, main_tname, main_alias_name,main_pk)

        main_columns = columns_dunplicate(main_columns, main_pk)
        main_str = [pk2table_str(main_pk)]
        main_str.extend(str2table_str(main_columns))

        # 主表数据获取结束
        # ---- 绘制主表
        render_table(main_alias_name, main_tname,main_str)

        # ----- 获取子表数据
        def get_table_struct(table_info):
            table_columns = table_info[0]  # 字段
            table_name = table_info[1]  # 表名
            table_pk = table_info[2]    # 主键
            table_alias = table_info[3]  # 表的别名
            table_res = table_info[4]  # 表的关联关系
            table_edge = re.findall(r'(\w+?)\.[\w =]+?(\w+?)\.', table_res, re.S | re.I | re.M)[0]  # 表的连线
            table_columns = table_columns.replace('\n', '')
            table_columns = re.split(r'\s*?,\s*?', table_columns)  # 分割
            table_columns = [re.sub(r'[\s|\n|\'|`]', '', colum) for colum in table_columns]  # 清洗
            return [table_columns, table_name, table_alias, table_res, table_edge,table_pk]

        # 处理所有的子表
        sub_list = re.findall(r'join\s+?\(\s+?select\s+?(.*?)FROM\s+?([\w\.]+?)\s+?.*?--\s*?mini_size:\s*?([\w, ]+)\s.*?\)\s*?(\w+?)\s*?on\s+?([\w\.= ]+)'
                      ,subtable[0],re.S|re.I|re.M)

        sub_table_list = []

        for index in range(len(sub_list)):
            print(get_table_struct(sub_list[index]))
            sub_table_list.append(get_table_struct(sub_list[index]))

        # 批量渲染
        for sub_table in sub_table_list:
            table_pk = str(sub_table[5]).replace(" ","")
            table_colums = columns_dunplicate(sub_table[0], table_pk)
            table_str = [pk2table_str(table_pk)]
            table_str.extend(str2table_str(table_colums))
            render_table(re.sub('\s', '', sub_table[2]), sub_table[1],table_str)

        # 主表和目标表
        s.edge(main_alias_name, target_tname)  # arrowhead = 'None'

        # 设置字体大小
        s.attr(fontsize='20')

        # 子表关联关系
        for index in range(len(sub_table_list)):
            table_edge(sub_table_list[index][4][1], sub_table_list[index][4][0], label=sub_table_list[index][3])

        s.view(filename=target_tname, directory=TARGET_PATH,cleanup=True,quiet_view=True)

def hive2ER(m_file):
    if (os.path.isdir(m_file)):
        listdir = os.listdir(m_file)
        for file in listdir:
            hql_file_to_ER(m_file + '/' + file)
    else:
        hql_file_to_ER(m_file)

if __name__ == '__main__':
    # 拿命令行参数
    m_file = str(sys.argv[1])
    if len(sys.argv) > 2:
        TARGET_PATH = str(sys.argv[2])

    hive2ER(m_file)






