import copy

from core.HqlParse import HqlParse
import re
import datetime


def convert2mysql_type(value,index):
    result = value
    if value.upper() == "STRING" and index < 70:
        result = 'varchar(255)'
    elif value.upper() == "STRING" and index > 70:
        result = 'TEXT'
    return result


def my_format(column_name, coumn_type, comment):
    mcolumn = column_name + str('\t' * (6 - (len(column_name) + 1) // 4))
    return ',{0}\t{1}\tCOMMENT\t{2}'.format(mcolumn, coumn_type, comment)


def my_format2(column_name, coumn_type, comment):
    mcolumn = column_name + str('\t' * (6 - (len(column_name) + 2) // 4))
    return '{0}\t--{2}'.format(mcolumn, coumn_type, comment)

def my_format3(column):
    column_name = column.split('--')[0].strip()
    comment = column.split('--')[1].strip()
    mcolumn = column_name + str('\t' * (6 - (len(column_name) + 1) // 4))
    return '{0}\t--{1}'.format(mcolumn, comment)



class SQLBuilder(object):
    def __init__(self, sql_statement):
        self._tables = []

    # 生成检测数据发散的SQL脚本
    def repeat_judge(self,sql_statement):
        sql_statement.replace('`','')
        parse = HqlParse(sql_statement)
        insert_info = parse.insert_info
        str_list = ['set hive.mapred.mode=strict;']
        for stmt in insert_info:
            tokens = stmt['sub_tables'][1:]
            idx = 1
            for token in tokens:
                table_alias = token['table']['table_alias']
                res_columns = []
                table_name = token['table']['table_name']
                # print(token['res'][0].split(table_alias)[1])
                for res in token['res']:
                    res_column = re.findall(r'\.?(\w+)\s*?', res.split(table_alias)[1], re.S | re.I)[0]
                    res_columns.append(res_column)
                str = '''
SELECT 
    "{0}",*
    FROM (
        SELECT 
            {1}
            ,COUNT(1) OVER(PARTITION BY {1}) AS ct
        FROM {2} 
    ) {0} WHERE {0}.ct > 1;
                '''.format(table_alias, ",".join(res_columns), table_name)
                #print(str)
                str_list.append(str)
        return '\n'.join(str_list)

    # 构建测试报告脚本
    def build_test_sql(self, sql_statement):
        parse = HqlParse(sql_statement)
        insert_info = parse.insert_info[0]
        target_table_name = insert_info['target_table_name']
        main_table_name = insert_info['sub_tables'][0]['table']['table_name']
        sql_list = []
        # 检验乱码
        sql_list.append('SELECT * FROM {0} LIMIT 500;'.format(target_table_name))
        sql_list.append('''SELECT "T1",COUNT(1) FROM {0} 
UNION ALL
SELECT "T2",COUNT(1) FROM {1};
        '''.format(target_table_name,main_table_name))
        # show table
        sql_list.append('show create table {0};'.format(target_table_name))
        return '\n'.join(sql_list)

    # Hive 表转 MySQL 表
    def hive2Mysql(self,sql_statement):
        parse = HqlParse(sql_statement)
        tables = parse.tables
        table_str = ''
        for table in tables:
            tablename = table['table_name'][table['table_name'].rfind('.') + 1:]
            columns = []
            table_comment = table['table_comment']
            index = 1
            for column in table['definitions']:
                # 字段名
                column_name = column[0].value
                # 字段类型
                coumn_type = convert2mysql_type(column[1].value,index)
                index += 1
                # 字段注释
                comment = column[3].value
                columns.append(my_format(column_name, coumn_type, comment))
            columns[0] = columns[0].replace(',','\t')
            table_str += """
CREATE TABLE IF NOT EXISTS {0} (
 {1}
    )COMMENT '{2}'
;
    """.format(tablename,"\n\t".join(columns),table_comment)
        return table_str

    # 多表联合字段去重
    def column_dumplicate(self, sql_statement,hostname,now):
        parse = HqlParse(sql_statement)
        tables = parse.tables
        columns = {}
        index = 1
        columns_comment = []
        table_dic = {}
        for table in tables:
            tablename = table['table_name']
            tablecomment = table['table_comment']
            alias_columns = []
            for column in table['definitions']:
                # 字段名
                column_name = column[0].value
                # 字段类型
                coumn_type = column[1].value
                # 字段注释
                comment = column[3].value
                m_column = my_format(column_name, coumn_type, comment)
                m_column__ = [column_name, my_format2(column_name, coumn_type, comment)]
                columns[column_name] = m_column
                alias_columns.append(m_column__)

            table_dic['t' + str(index)] = {'columns':alias_columns,'tablename':tablename,'tablecomment':tablecomment}
            index += 1
        # 表字段去重
        # TODO 适配 ` 号
        dump_keys = []
        for key in table_dic.keys():
            # 每张表与前面的表比对去重 删掉自己的重复的
            c1 = table_dic[key]['columns']
            c1_new = []
            if len(dump_keys) == 0:
                for c in c1:
                    c1_new.append(c[0])
                dump_keys = copy.deepcopy(c1_new)
                continue
            for c in c1:
                if c[0] not in dump_keys:
                    c1_new.append([c[0],c[1]])
                    dump_keys.append(c[0])
            table_dic[key]['columns'] = list(c1_new)
        sub_table_str = []
        flag = 0
        for key in table_dic.keys():
            sub_table_columns = []
            # 添加数据
            for co in table_dic[key]['columns']:
                # SELECT
                columns_comment.append(my_format3('{0}.{1}'.format(key,co[1].strip())))
                # 子表的columns
                sub_table_columns.append(co[1].strip())

            #print('\n'.join(sub_table_columns),table_dic[key]['tablename'])
            if not flag:
                ss = """ (
         select
         {0}
         from {1} -- min_size:   comment:{3}
         -- where ds = ' ' 
         ) {2}
    
         """.format('\n\t\t ,'.join(sub_table_columns), table_dic[key]['tablename'], key,table_dic[key]['tablecomment'])
                flag = 1
            else:
                ss = """LEFT JOIN(
         select
         {0}
         from {1} -- min_size:   comment: {3}
         -- where ds = ' ' 
         ) {2}
         ON
         """.format('\n\t\t ,'.join(sub_table_columns), table_dic[key]['tablename'], key,table_dic[key]['tablecomment'])
            sub_table_str.append(ss)
        columns_values = list(columns.values())
        columns_values[0] = columns_values[0].replace(',', '\t')
        columns_str = '\n \t'.join(columns_values)
        columns_comment[0] = '\t' + str(columns_comment[0])
        columns_comment_str = '\n\t,'.join(list(columns_comment))
        final = """----------------------------------------------
-- @ Output：db_name.table_name
-- @ Desc：{5}
-- @ Primary Key： 
-- @ Author：{3}
-- @ Create Time：{4}
-- @ Modify Time：({3}，{4}，创建表)
-- @ Comment：{5}
----------------------------------------------

CREATE TABLE IF NOT EXISTS XXXXX (
{0}
 \t,etl_time timestamp COMMENT 'etl_time'
)COMMENT '{5}'
-- 要不要分区？partitioned by (ds string)
ROW FORMAT DELIMITED STORED AS orc TBLPROPERTIES('orc.compression'='SNAPPY')
;
-- 分区检查
set hive.mapred.mode = strict;

insert overwrite table XXXXXX 分区？partition(ds = ' ' )
SELECT 
{1}
 \t,current_timestamp as etl_time 
from 
{2}
    """.format(columns_str, columns_comment_str, '\n'.join(sub_table_str), hostname, now,table_dic['t1']['tablecomment'])
        final += '\n;'
        return final

    def column_none_dumplicate(self, sql_statement, hostname, now):
        parse = HqlParse(sql_statement)
        tables = parse.tables
        columns = {}
        index = 1
        columns_comment = []

        table_dic = {}
        for table in tables:
            tablename = table['table_name']
            tablecomment = table['table_comment']
            alias_columns = []
            for column in table['definitions']:
                # 字段名
                column_name = column[0].value
                # 字段类型
                coumn_type = column[1].value
                # 字段注释
                comment = column[3].value
                m_column = my_format(column_name, coumn_type, comment)
                m_column__ = [column_name, my_format2(column_name, coumn_type, comment),m_column]
                #columns[column_name] = m_column
                alias_columns.append(m_column__)

            table_dic['t' + str(index)] = {'columns': alias_columns, 'tablename': tablename,
                                           'tablecomment': tablecomment}
            index += 1
        # 表字段去重
        # TODO 适配 ` 号
        dump_keys = []
        for key in table_dic.keys():
            # 每张表与前面的表比对去重 如果有重复的就加前缀
            c1 = table_dic[key]['columns']
            c1_new = []
            c1_columns = []
            if len(dump_keys) == 0:
                for c in c1:
                    c1_new.append(c[0])
                    c1_columns.append([c[0],c[1],c[1]])
                    columns[c[0]] = c[2]
                dump_keys = copy.deepcopy(c1_new)
                table_dic[key]['columns'] = list(c1_columns)
                continue
            for c in c1:
                if c[0] not in dump_keys:
                    c1_new.append([c[0],c[1],c[1]])
                    dump_keys.append(c[0])
                    columns[c[0]] = c[2]
                else:
                    tablename = table_dic[key]['tablename'][table_dic[key]['tablename'].rfind('.')+1:]
                    tablename = re.sub(r'\w+_\w+_(\w+)_\w+', r'\1',tablename, flags=re.M | re.I | re.S)
                    new_cname = '{0}_{1}'.format(tablename,c[0])
                    new_cname_all = '{0} as {1}'.format(c[0],str(c[1]).replace(c[0],new_cname))
                    c1_new.append([new_cname,new_cname_all,c[1]])
                    dump_keys.append(new_cname)
                    columns[new_cname] = c[2].replace(c[0],new_cname)
            table_dic[key]['columns'] = list(c1_new)
        sub_table_str = []
        flag = 0
        for key in table_dic.keys():
            sub_table_columns = []
            # 添加数据
            for co in table_dic[key]['columns']:
                # SELECT
                columns_comment.append(my_format3('{0}.{1}'.format(key, co[1].strip())))
                # 子表的columns
                sub_table_columns.append(co[2].strip())

            # print('\n'.join(sub_table_columns),table_dic[key]['tablename'])
            if not flag:
                ss = """ (
         select
         {0}
         from {1} -- min_size:   comment:{3}
         -- where ds = ' ' 
         ) {2}

         """.format('\n\t\t ,'.join(sub_table_columns), table_dic[key]['tablename'], key,
                    table_dic[key]['tablecomment'])
                flag = 1
            else:
                ss = """LEFT JOIN(
         select
         {0}
         from {1} -- min_size:   comment: {3}
         -- where ds = ' ' 
         ) {2}
         ON
         """.format('\n\t\t ,'.join(sub_table_columns), table_dic[key]['tablename'], key,
                    table_dic[key]['tablecomment'])
            sub_table_str.append(ss)
        columns_values = list(columns.values())
        columns_values[0] = columns_values[0].replace(',', '\t')
        columns_str = '\n \t'.join(columns_values)
        columns_comment[0] = '\t' + str(columns_comment[0])
        columns_comment_str = '\n\t,'.join(list(columns_comment))
        final = """----------------------------------------------
-- @ Output：db_name.table_name
-- @ Desc：{5}
-- @ Primary Key： 
-- @ Author：{3}
-- @ Create Time：{4}
-- @ Modify Time：({3}，{4}，创建表)
-- @ Comment：{5}
----------------------------------------------

CREATE TABLE IF NOT EXISTS XXXXX (
{0}
 \t,etl_time timestamp COMMENT 'etl_time'
)COMMENT '{5}'
-- 要不要分区？partitioned by (ds string)
ROW FORMAT DELIMITED STORED AS orc TBLPROPERTIES('orc.compression'='SNAPPY')
;
-- 分区检查
set hive.mapred.mode = strict;

insert overwrite table XXXXXX 分区？partition(ds = ' ' )
SELECT 
{1}
 \t,current_timestamp as etl_time 
from 
{2}
    """.format(columns_str, columns_comment_str, '\n'.join(sub_table_str), hostname, now,
               table_dic['t1']['tablecomment'])
        final += '\n;'
        return final

    # 查询语句批量生成
    def select_generate(self,sql_statement,query):
        parse = HqlParse(sql_statement)
        tables = parse.tables

        columns_comment = []
        table_str = ''
        for table in tables:
            tablename = table['table_name']
            tablecomment = table['table_comment']
            columns = []
            for column in table['definitions']:
                # 字段名
                column_name = column[0].value
                # 字段类型
                coumn_type = column[1].value
                # 字段注释
                comment = column[3].value
                columns.append(query.format(column_name))
            #columns[0] = columns[0][columns[0].find(',')+1:]
            partition = parse.get_partition_col(sql_statement)
            if partition:
                today = datetime.datetime.now()
                yesterday = today - datetime.timedelta(days=1)
                yesterday = yesterday.strftime('%Y%m%d')
                table_str += "SELECT {0} \n FROM {1} \n WHERE {2} = \'{3}\';\n".format('\n,'.join(columns), tablename,partition,yesterday)
            else:
                table_str += "SELECT {0} FROM {1};\n".format('\n,'.join(columns),tablename)
        return table_str

    # 多表组合union
    def table_union(self, sql_statement, hostname, now):
        sql_statement = sql_statement.replace('`', '')
        sql_statement = sql_statement.replace('"', '')
        parse = HqlParse(sql_statement)
        tables = parse.tables
        columns = {}
        index = 1
        columns_comment = []
        table_dic = {}
        for table in tables:
            tablename = table['table_name']
            tablecomment = table['table_comment']
            alias_columns = []
            for column in table['definitions']:
                # 字段名
                column_name = column[0].value
                # 字段类型
                coumn_type = column[1].value
                # 字段注释
                comment = column[3].value
                m_column = my_format(column_name, coumn_type, comment)
                m_column__ = [column_name, my_format2(column_name, coumn_type, comment)]
                columns[column_name] = m_column
                alias_columns.append(m_column__)

            table_dic['t' + str(index)] = {'columns': alias_columns, 'tablename': tablename,
                                           'tablecomment': tablecomment}
            index += 1
        # 表字段去重
        # TODO 适配 ` 号
        dump_keys = []
        for key in table_dic.keys():
            # 每张表与前面的表比对去重 删掉自己的重复的
            c1 = table_dic[key]['columns']
            c1_new = []
            if len(dump_keys) == 0:
                for c in c1:
                    c1_new.append(c[0])
                dump_keys = copy.deepcopy(c1_new)
                continue
            for c in c1:
                if c[0] not in dump_keys:
                    c1_new.append([c[0], c[1]])
                    dump_keys.append(c[0])
        #     table_dic[key]['columns'] = list(c1_new)
        #print(dump_keys)
        sub_table_str = []
        flag = 0
        for key in table_dic.keys():
            sub_table_columns = []
            table_columns_key = []
            for co in table_dic[key]['columns']:
                # SELECT
                columns_comment.append(co[1].strip())
                # 子表的columns
                table_columns_key.append(co[0].strip())
            # 添加数据
            for column in dump_keys:
                if column not in table_columns_key:
                    sub_table_columns.append("NULL AS {0}".format(column))
                else:
                    sub_table_columns.append(column)

            if not flag:
                ss = """ 
 select
        {0}
    \t,current_timestamp as etl_time 
 from {1} -- min_size:   comment:{3}
 where ds = '{4}'""".format('\n\t\t ,'.join(sub_table_columns), table_dic[key]['tablename'], key,
                    table_dic[key]['tablecomment'],'${bdp.system.bizdate}')
                flag = 1
            else:
                ss = """
UNION ALL
select
        {0}
    \t,current_timestamp as etl_time 
from {1} -- min_size:   comment: {3}
where ds = '{4}'""".format('\n\t\t ,'.join(sub_table_columns), table_dic[key]['tablename'], key,
                    table_dic[key]['tablecomment'],'${bdp.system.bizdate}')
            sub_table_str.append(ss)
        columns_values = list(columns.values())
        columns_values[0] = columns_values[0].replace(',', '\t')
        columns_str = '\n \t'.join(columns_values)
        columns_comment[0] = '\t' + str(columns_comment[0])
        columns_comment_str = '\n\t,'.join(list(columns_comment))
        final = """----------------------------------------------
-- @ Output：db_name.table_name
-- @ Desc：{5}
-- @ Primary Key： 
-- @ Author：{3}
-- @ Create Time：{4}
-- @ Modify Time：({3}，{4}，创建表)
-- @ Comment：{5}
----------------------------------------------

CREATE TABLE IF NOT EXISTS XXXXX (
{0}
 \t,etl_time timestamp COMMENT 'etl_time'
)COMMENT '{5}'
partitioned by (ds string)
ROW FORMAT DELIMITED STORED AS orc TBLPROPERTIES('orc.compression'='SNAPPY')
;
-- 分区检查
set hive.mapred.mode = strict;

insert overwrite table XXXXXX partition(ds = '{6}')

{2}
    """.format(columns_str, columns_comment_str, '\n'.join(sub_table_str), hostname, now,
               table_dic['t1']['tablecomment'],'${bdp.system.bizdate}')
        final += '\n;'
        return final




SQL = """create external table hive_ods.ods_order_info (
id string COMMENT '订单号',
final_total_amount decimal(10,2) COMMENT '订单金额',
order_status string COMMENT '订单状态',
user_id string COMMENT '用户 id',
out_trade_no string COMMENT '支付流水号',
create_time string COMMENT '创建时间',
operate_time string COMMENT '操作时间',
province_id string COMMENT '省份 ID',
benefit_reduce_amount decimal(10,2) COMMENT '优惠金额',
original_total_amount decimal(10,2) COMMENT '原价金额',
feight_fee decimal(10,2) COMMENT '运费'
) COMMENT '订单表'
PARTITIONED BY (dt string)
;
create external table hive_ods.ods_order_detail(
id string COMMENT '订单编号',
order_id string COMMENT '订单号',
user_id string COMMENT '用户 id',
sku_id string COMMENT '商品 id',
sku_name string COMMENT '商品名称',
order_price decimal(10,2) COMMENT '商品价格',
sku_num bigint COMMENT '商品数量',
create_time string COMMENT '创建时间'
) COMMENT '订单详情表'
PARTITIONED BY (dt string);

"""

sql_builder = SQLBuilder(SQL)
print(sql_builder.table_union(SQL,hostname='gw11',now='20210605'))



