from core.HqlParse import HqlParse

sql = ""  # SQL脚本可以通过参数传入

parse = HqlParse(sql)  # 构建解析器对象即可解析SQL
tables = parse.tables  # tables 属性可以拿到解析好的建表语句SQL ，包含字段名 字段类型 字段注释 表名 库名 表注释
insert_info = parse.insert_info # insert_info 属性可以拿到解析好的INSERT 语句，包含 目标表、主表、表的关联关系、子表、子表字段
