#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os, sys
import re
from graphviz import Digraph, Graph

# 配置最多能显示的字段数量
from core.HqlParse import HqlParse

MAX_NUM_OF_COLUMS = 16
TARGET_PATH = 'D:/target/er'


# 脚本使用方法
# 1.以文件夹作为参数 , 第二个参数为保存路径（默认为./target）  python HQL_ER_END.py   ../to_path  /save_path
# 2.使用hql文件作为参数 第二个参数为保存路径（默认为./target） python HQL_ER_END.py   ./test.hql  /save_path

# 字段去重
def columns_dunplicate(columns, column):
    new_col = []
    if not isinstance(column,list):
        column = re.sub(r'\s*(\w+)?\.?(\w+)\s*.*',r'\2',column,flags=re.M|re.S|re.I)
        for col in columns:
            judge_str = re.sub(r'\s*(\w+)?\.?(\w+)\s*.*',r'\2',col,flags=re.M|re.S|re.I)
            if column != judge_str:
                index = re.search(r'\s+as\s+', col, re.I)
                if (index):
                    new_col.append(col[index.span()[1]:])
                else:
                    new_col.append(col)
    elif isinstance(column,list):
        for cl in column:
            cl = re.sub(r'\s*(\w+)?\.?(\w+)\s*.*', r'\2', cl, flags=re.M | re.S | re.I)
            for col in columns:
                judge_str = re.sub(r'\s*(\w+)?\.?(\w+)\s*.*',r'\2',col,flags=re.M|re.S|re.I)
                if cl != judge_str:
                    index = re.search(r'\s+as\s+', col, re.I)
                    if (index):
                        new_col.append(col[index.span()[1]:])
                    else:
                        new_col.append(col)
    return new_col

def columns_dunplicate_2(columns, column):\

    if not isinstance(column, list):
        remove_col = None
        for col in columns:
            if column.strip() in col or column == col:
                remove_col = col
                break
        if remove_col:
            columns.remove(remove_col)
    elif isinstance(column,list):
        for cl in column:
            remove_col = None
            for col in columns:
                if cl.strip() in col or cl == col:
                    remove_col = col
                    break
            if remove_col:
                columns.remove(remove_col)
    return columns


def hql_file_to_ER(hql,sql_str):
    s = Digraph('structs', filename='structs_revisited.gv',
                node_attr={'shape': 'record'}, format='jpg')

    # 字符串转HTML表格结构
    def str2table_str(columns):
        # 限制字段个数
        const_columns = columns[0:MAX_NUM_OF_COLUMS]
        return [ '<TR><TD COLSPAN="3">{}</TD></TR>'.format(column)   for column in const_columns]

    #  主键字段HTML
    def pk2table_str(PK):
        # 限制字段个数
        pk_list = []
        if isinstance(PK,list):
            for key in PK:
                pk_list.append('<TR> <TD COLSPAN = "3" align = "CENTER"><B> PK： {} </B></TD> </TR>'.format(key))
        else:
            return '<TR> <TD COLSPAN = "3" align = "CENTER"><B> PK： {} </B></TD> </TR>'.format(PK)
        return '\n'.join(pk_list)

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

    # 目标表
    hqlparse = HqlParse(hql)
    insert_info = hqlparse.insert_info[0]
    target_tname = insert_info['target_table_name']
    target_t_columns = insert_info['target_columns']
    target_pk = re.findall(r'--\s*?@\s*?Primary Key[：:]\s*?([\w,]+)\s*?', sql_str, re.S | re.M | re.I)
    if target_pk:
        target_pk = target_pk[0].split(',')
    else:
        target_pk = target_t_columns[0]
    # 去重
    target_t_columns = columns_dunplicate(target_t_columns, target_pk) if target_pk else target_t_columns
    # 主键的HTML 字符串
    pk_str = pk2table_str(target_pk)
    print(target_tname, target_t_columns,target_pk)

    main_node_str = []
    main_node_str.append(pk_str)
    main_node_str.extend(str2table_str(target_t_columns))
    render_table('', target_tname, main_node_str)

    # -----  处理目标表结束

    # ----- 处理主表数据
    if not insert_info['sub_tables']:
        # 没有主表
        s.render(filename=target_tname, directory=TARGET_PATH, cleanup=True)
        return target_tname
    main_table = insert_info['sub_tables'][0]['table']

    main_tname = main_table['table_name']
    main_columns = main_table['columns']
    # new_columns = []
    # for column in main_columns:
    #     index = re.search(r'\s+as\s+', column, re.I)
    #     if (index):
    #         column = column[index.span()[1]:]
    #         print(column)
    #     new_columns.append(column)
    # main_columns = new_columns

    main_alias_name = main_table['table_alias']

    # main_pk = \
    #     re.findall(r'.*?from\s*?\(\s*?select.*?FROM.*?--\s*?mini_size:\s*?([\w, ]+?)\s', subtable[0], re.S | re.I)[
    #         0].replace(' ', '')

    # print(main_columns, main_tname, main_alias_name, main_pk)
    main_pk = hqlparse.get_pk_from_table(main_tname)
    if main_pk:
        main_pk = main_pk.split(',')
    else:
        main_pk = main_columns[0]
        try:
            main_columns.remove(main_pk)
        except:
            pass
    main_columns = columns_dunplicate(main_columns, main_pk)
    main_str = [pk2table_str(main_pk)]
    main_str.extend(str2table_str(main_columns))

    # 主表数据获取结束
    # ---- 绘制主表
    render_table(main_alias_name, main_tname, main_str)

    # ----- 获取子表数据
    def get_table_struct(table_info):
        table_res = table_info['res']
        table_info = table_info['table']
        table_columns = table_info['columns']
        table_name = None
        if not(table_columns) or len(table_columns) == 0:
            return None
        if not table_info['table_name']:
            try:
                table_name = table_info['subtable'][0]['table_name']
            except:
                pass
        else:
            table_name = table_info['table_name']

        table_pk = hqlparse.get_pk_from_table(table_name) if table_name else  None
        if table_pk:
            table_pk = table_pk.split(',')
        else:
            table_pk = table_columns[0]
            try:
                table_columns.remove(table_pk)
            except:
                pass
        table_alias = table_info['table_alias']
        table_edge_list = []
        table_res_etl = []
        for res in table_res:
            table_edge = re.findall(r'(\w+?)\.[\w =]+?(\w+?)\.', res, re.S | re.I | re.M)  # 表的连线
            res = re.sub(r'(.*?)(\s+?--.*)',r'\1', res, re.S | re.I | re.M)
            table_res_etl.append(res)
            if table_edge:
                table_edge_list.append(table_edge[0])
        #print(table_res_etl)
        table_columns = [re.sub(r'[\s|\n|\'|`]', '', colum) for colum in table_columns]  # 清洗
        table_columns = columns_dunplicate_2(table_columns,table_pk)
        return [table_columns, table_name, table_alias, table_res_etl, table_edge_list, table_pk]

    # 处理所有的子表
    sub_list = insert_info['sub_tables'][1:]

    sub_table_list = []

    for index in range(len(sub_list)):
        #print(get_table_struct(sub_list[index]))
        sub_table_list.append(get_table_struct(sub_list[index]))

    try:
        sub_table_list.remove(None)
    except:
        pass
    # 批量渲染
    for sub_table in sub_table_list:
        table_pk = sub_table[5]
        table_colums = sub_table[0]
        table_str = [pk2table_str(table_pk)]
        table_str.extend(str2table_str(table_colums))
        render_table(re.sub('\s', '', sub_table[2]), sub_table[1], table_str)

    # 主表和目标表
    s.edge(main_alias_name, target_tname)  # arrowhead = 'None'

    # 设置字体大小
    s.attr(fontsize='20')

    # 子表关联关系
    for index in range(len(sub_table_list)):
        for (sub_table_res,sub_table_res_str) in zip(sub_table_list[index][4],sub_table_list[index][3]):
            table_edge(sub_table_res[1], sub_table_res[0], label=sub_table_res_str)

    s.render(filename=target_tname,directory=TARGET_PATH,cleanup=True)
    #s.view(filename=target_tname, directory=TARGET_PATH, cleanup=True,quiet=False, quiet_view=False)
    return target_tname


def read_file_to_sql(m_file):
    with open(m_file, encoding='utf-8') as f:
        hql = f.read()
        for sql in HqlParse.get_insert_sql_list(hql):
            hql_file_to_ER(sql.value)

def hive2ER(m_file):
    if (os.path.isdir(m_file)):
        listdir = os.listdir(m_file)
        for file in listdir:
            read_file_to_sql(m_file + '/' + file)
    else:
        read_file_to_sql(m_file)

def hive2ER_from_str(sql_str):
    filename_list  =  []
    for sql in HqlParse.get_insert_sql_list(sql_str):
        filename_list.append(hql_file_to_ER(sql.value,sql_str))
    return filename_list



if __name__ == '__main__':
    # 拿命令行参数
    m_file = str(sys.argv[1])
    if len(sys.argv) > 2:
        TARGET_PATH = str(sys.argv[2])

    hive2ER(m_file)