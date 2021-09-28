# 提取建表语句的所有Columns
import sqlparse
import re
from sqlparse.tokens import Keyword, DML, DDL


class HqlParse(object):
    def __init__(self, sql_statement):
        self._tables = []
        self._insert_info = []
        self.sql = sqlparse.format(sql_statement, keyword_case='upper')
        # self.sql = sql_statement
        self.sql = self.stripped()
        self.clean_sql()
        self._parsed = sqlparse.parse(self.stripped())
        for statement in self._parsed:
            self.__extract_from_token(statement)

    def clean_sql(self):
        self.sql = re.sub(r',\s*?\w+\(.*\)\s*?OVER\([A-Za-z0-9_ \n,]*?\)\s+(?:(as)?\s*?)(\w+)', r',\2', self.sql,
                          flags=re.M | re.I)
        self.sql = re.sub(r'\s*?case.*?as\s+(\w+)', r' \1', self.sql, flags=re.M | re.I | re.S)
        self.sql = re.sub(r'(\w+\s+?)as(\s+?\w+)', r'\2', self.sql, flags=re.M | re.S | re.I)
        return self.sql

    def stripped(self):
        return self.sql.strip(' \t\n;')

    def __extract_from_token(self, statement):
        if statement.get_type() == "CREATE":
            # 拿字段和表名
            _, par = statement.token_next_by(i=sqlparse.sql.Parenthesis)
            self._tables.append({'table_name': self.extract_table_name(statement),
                                 'columns': self.extract_str_definitions(par),
                                 'definitions': self.extract_definitions(par)})
        elif statement.get_type() == "INSERT":
            # 拿目标表、子表、字段、关联关系
            self._insert_info.append(self.extract_insert_table_info(statement))
        return None

    @property
    def tables(self):
        return self._tables

    @property
    def all(self):
        return self._tables, self._insert_info

    # 返回的结构：
    # {'target_columns': list[str],
    # 'target_table_name':target_table_name,
    # 'sub_tables':
    #           [{'table':
    #               {'table_name': 'sany_dw.dwd_crm_crmd_orderadm_i_df', 'table_alias': 't1',
    #               'columns': ["number_int  -- '凭证中的项目编号'", 'guid', "header   -- 'crm 订单对象的 guid'", "product  -- '产品的内部唯一标识'",'itm_type  -- 行项目类型'],
    #               'subtable': []}, 'res': []}
    #            },...]
    @property
    def insert_info(self):
        return self._insert_info

    def extract_definitions(self, token_list):
        # assumes that token_list is a parenthesis
        definitions = []
        tmp = []
        par_level = 0
        for token in token_list.flatten():
            if token.is_whitespace:
                continue
            elif token.match(sqlparse.tokens.Punctuation, '('):  # 语句开始
                par_level += 1
                continue
            elif re.match(r'\s*?--.*?', str(token)):  # 判断是否是注释
                continue
            if token.match(sqlparse.tokens.Punctuation, ')'):  # 语句的结束
                if par_level == 0:
                    break
                else:
                    par_level += 1
            elif token.match(sqlparse.tokens.Punctuation, ','):  # 一个字段定义的结束
                if tmp:
                    definitions.append(tmp)
                tmp = []
            else:
                tmp.append(token)
        if tmp:
            definitions.append(tmp)
        return definitions

    def extract_str_definitions(self, token_list):
        definitions = self.extract_definitions(token_list)
        columns = []
        for column in definitions:
            columns.append(' {name!s:12} {definition}'.format(
                name=column[0], definition=' '.join(str(t) for t in column[1:])))
        return columns

    def extract_table_name(self, parsed):
        _, par = parsed.token_next_by(i=sqlparse.sql.Identifier)
        return par.value

    def extract_table_comment(sql):
        comment = re.findall(r"\s*\)\s*?COMMENT\s+\'(.*?)'\s+", sql, re.M | re.I | re.S)
        if comment:
            return comment[0]
        else:
            return None

    def extract_insert_table_info(self, stmt):
        target_columns = []
        target_table_name = ''
        sub_tables = []
        if stmt.get_type() == "INSERT":

            idx = 0
            # 拿目标表
            idx, par = stmt.token_next_by(idx=idx, i=sqlparse.sql.Identifier)
            target_table_name = par.value
            c_idx = 1
            while (c_idx):
                c_idx, columns = stmt.token_next_by(idx=c_idx, i=sqlparse.sql.IdentifierList)
                if columns:
                    for column in columns:
                        if not (column.match(sqlparse.tokens.Punctuation, ',')):
                            # print(column.value.strip())
                            target_columns.append(column.value.strip())
            while (idx):
                idx, par = stmt.token_next_by(idx=idx, i=sqlparse.sql.Identifier)
                # print(idx, par)
                tmp = {}
                if par and "SELECT" in par.value:
                    # print(self.get_subtable_info(par))
                    tmp['table'] = self.get_subtable_info(par)
                else:
                    continue
                pre_idx = idx
                next_idx = idx
                if idx:
                    next_idx, _ = stmt.token_next_by(idx=idx, i=sqlparse.sql.Identifier)
                if not (pre_idx):
                    break
                if not (next_idx):
                    next_idx = 9999
                com_idx = pre_idx + 1
                res_list = []
                while com_idx and (com_idx < next_idx):
                    com_idx, com_par = stmt.token_next_by(idx=com_idx, end=next_idx + 1, i=sqlparse.sql.Comparison)
                    # print(com_idx, com_par)
                    if (com_par):
                        res_list.append(com_par.value)
                tmp['res'] = res_list
                sub_tables.append(tmp)

        return {'target_columns': target_columns, 'target_table_name': target_table_name, 'sub_tables': sub_tables}

    # 去掉 as 关键字左边的数据拿字段名
    def remove_as(self, str):
        str = re.sub(r'.*\s+AS\s+(\w+)\s*?.*', r'\1', str, flags=re.M | re.S | re.I)
        return str

    def get_insert_sql_list(stmt):
        parsed = sqlparse.format(stmt, keyword_case='upper')
        parsed = sqlparse.parse(parsed)
        sql_list = []
        for statement in parsed:
            if statement.get_type() == "INSERT":
                sql_list.append(statement)
        return sql_list

    def get_create_sql_list(stmt):
        parsed = sqlparse.format(stmt, keyword_case='upper')
        parsed = sqlparse.parse(stmt)
        create_list = []
        for statement in parsed:
            if statement.get_type() == "CREATE":
                create_list.append(statement)
        return create_list

    def get_pk_from_table(self, table_name):
        key_list = re.findall(r'.+FROM\s+' + table_name + '\s+?--\s*mini_?size:\s*([\w, ]+?)\s+', self.sql,
                              re.S | re.I | re.M)
        if key_list:
            return key_list[0]
        return None

    # 获取表的详情
    def get_subtable_info(self, identifier):
        table_name = ''
        table_alias = ''
        table_columns = []
        subtable = []
        # 别名
        _, alias = identifier.token_next_by(i=sqlparse.sql.Identifier)
        table_alias = alias.value if alias else None
        # print(table_alias)
        _, parenthesis = identifier.token_next_by(i=sqlparse.sql.Parenthesis)
        if parenthesis:
            # 字段名
            c_idx = 1
            c_idx, columns = parenthesis.token_next_by(idx=c_idx, i=sqlparse.sql.IdentifierList)
            # try:
            #     for column in columns:
            #         if not (column.match(sqlparse.tokens.Punctuation, ',')) and not (
            #         re.match(r'^\s*$', column.value.strip())):
            #             # print(column.value.strip())
            #             table_columns.append(column.value.strip())
            #         if column.match(sqlparse.tokens.Punctuation, ')'):  # 语句的结束
            #             break
            # except:
            #     print(parenthesis.ttype,parenthesis.value)
            #     c_idx, columns = parenthesis.token_next_by(idx=1, i=sqlparse.sql.IdentifierList)
            #     for token in parenthesis.tokens:
            #         print(type(token),token.value)
            if c_idx:
                for column in columns:
                    if not (column.match(sqlparse.tokens.Punctuation, ',')) and not (
                            re.match(r'^\s*$', column.value.strip())):
                        # print(column.value.strip())
                        table_columns.append(column.value.strip())
                    if column.match(sqlparse.tokens.Punctuation, ')'):  # 语句的结束
                        break
            else:
                # 只有一条
                c_idx = 1
                c_idx, columns = parenthesis.token_next_by(idx=c_idx, i=sqlparse.sql.Identifier)
                table_columns.append(self.remove_as(columns.value))
            # 表名
            c_idx, table = parenthesis.token_next_by(idx=c_idx, i=sqlparse.sql.Identifier)
            # 判断是否有子表 会存在有些子表拿不到的情况 因为这里写死了
            # flatten = list(table.flatten())
            tokens = table.tokens
            parenthesis_ = table.token_next_by(i=sqlparse.sql.Parenthesis)[1]
            a = [i if i % 2 == 0 else 'qi' for i in range(10)]
            m_tokens = []
            for token in tokens:
                if not token.is_whitespace:
                    m_tokens.append(token)

            # print(len(tokens)) not
            if len(m_tokens) > 7 or parenthesis_:
                subtable.append(self.get_subtable_info(table))
                table_name = re.findall(r'.*?FROM\s+?([\w\.]+?)\s+?', table.value, re.S | re.I)
                if table_name:
                    table_name = table_name[0]
            else:
                # print(table)
                table_name = table.get_parent_name() + '.' + table.get_real_name()
                # print(table_name)

        return {'table_name': table_name, 'table_alias': table_alias, 'columns': table_columns, 'subtable': subtable}


if __name__ == '__main__':
    SQL = """insert overwrite table sany_dim.dim_prd_mdm_mtrl_df PARTITION(ds = '${IncStartDate}') --按照逻辑关系最新输出表
 select
  t12.depth         --'层级'
  ,t12.type_no        --'分类编码'
  ,t12.type_descs        --'分类描述'
  ,t12.level1_id        --'一级分类ID'
  ,t12.level1_desc       --'一级分类描述'
  ,t12.level2_id        --'二级分类ID'
  ,t12.level2_desc       --'二级分类描述'
  ,t12.level3_id        --'三级分类ID'
  ,t12.level3_desc       --'三级分类描述'
  ,t12.level4_id        --'四级分类ID' 
  ,t1.ti_material_application_id    --'物料主表ID'
  ,t1.tm_clas_id        --'物料分类表ID'
  ,t1.process_id        --'流程编号'
  ,t1.type AS apply_type      --'申请类型'
  ,t3.entry_name AS apply_type_desc   --'申请类型名称'
  ,t1.material_status       --'物料状态'
  ,t4.entry_name AS tm_status_desc   --'物料状态名称'
  ,t1.refence_material_no      --'参考物料编码'
  ,t1.tz_code         --'特征码'
  ,t1.material_no        --'物料编码'
  ,t1.name AS material_name     --'物料名称'
  ,t1.groes         --'规格型号'
  ,t1.normt         --'标准代号'
  ,t1.description        --'物料描述'
  ,t1.eng_name        --'英文名称'
  ,t1.is_pdm         --'PLM物料视图'
  ,t1.basic_unit        --'基本单位'
  ,t1.material_spec       --'材料规格'
  ,t1.erp_material_status      --'ERP跨工厂状态'
  ,t6.entry_name AS erp_material_status_desc --'ERP跨工厂状态名称'
  ,t1.material_type       --'物料类型'
  ,t7.entry_name AS material_type_desc  --'物料类型名称'
  ,t1.product_group       --'产品组(外购件)'
  ,t8.entry_name AS product_group_name  --'产品组(外购件)名称'
  ,(case when t1.type='2' or t1.type='3' then t1.`size` else t1.ckg end) as ckg   --'长*宽*高' --20210330张逸夫
  ,t1.net_weight        --'净重'
  ,t1.weight_unit        --'重量单位'
  ,t1.volume         --'体积'
  ,t1.volume_unit        --'体积单位'
  ,t9.entry_name AS volume_unit_name   --'体积单位名称'
  ,t1.consumable_part       --'是否易损件(Y代表是)'
  ,t1.maintenance_part      --'是否保养件(Y代表是)'
  ,t1.notice_part        --'是否公告物料(Y代表是)'
  ,t1.repair_package       --'是否维修包(Y代表是)'
  ,t1.random_accessory_package    --'是否随机附件包(Y代表是)'
  ,t1.zycz         --'主要材质'
  ,t1.zyt          --'用途'
  ,t1.zgzyl         --'工作原理'
  ,t1.purchase_tech_type      --'采购技术要求'
  ,t1.original_material_no     --'旧物料号(原物料编码)'
  ,t1.create_time        --'创建日期'
  ,t1.tzk_code_cn        --'特征码含义'
  ,t1.reason         --'申请理由'
  ,t1.checkman        --'审核人'
  ,t1.doc_version        --'版本号'
  ,t1.snapshot_code       --'快照号'
  ,t1.operation        --'操作'
  ,t1.category_code     --'主体/总成(1主体 2总成)'
  ,t1.zeinr     --'图号'
  ,t1.gnms     --'功能描述'
  ,t1.is_ckd     --'CKD类型'
  ,t1.initial_target_cost     --'初始目标成本'
  ,t1.is_config     --'是否可配置物料(Y代表是)'
  ,t1.pdm_material_status     --'PLM物料状态'
  ,t1.pdm_product_lib     --'PLM产品库'
  ,t1.oid     --'PLM一级文件夹'
  ,t1.oid_second     --'PLM二级文件夹'
  ,t1.product_level       --'产品层次'
  ,t11.prod_level_no       --'产品层次代码'
  ,t11.prod_level_desc    --'产品层次短名称'
  ,t11.prod_level_s_desc    --'产品层次长名称'
  ,t11.PROCESS_STATUS       --'流程状态'
  ,t10.id AS jsxy_serial_id     --'序号'
  ,t10.create_username AS jsxy_create_person --'创建者'
  ,t10.create_time AS jsxy_create_datetime --'创建日期'
  ,t10.jsxy_name AS jsxy_technical_protocal --'技术协议文档'
  ,t10.pdf_url as jsxy_url      --'技术文档路径'
  ,t1.creater_account       --'MDM申请人账号'
  ,t1.creater_name       --'MDM申请人姓名'
  ,t13.matkl         --'物料组代码'
  ,t13.meins         -- string COMMENT '基本计量单位'
  ,t13.mtart         -- string COMMENT '物料类型代码'
  ,t13.spart         -- string COMMENT '产品组代码'
  ,t13.ntgew         -- string COMMENT '净重'
  ,t13.gewei         -- string COMMENT '重量单位'
  ,t13.wgcc         -- string COMMENT '外观尺寸'
  ,t13.zmeabm         -- string COMMENT '尺寸单位'
  ,t14.wgbez         --'物料组名称'
  ,tm.code         -- '工厂代码'
  ,current_timestamp as etl_time    --timestamp comment'etl任务完成时间'
from 
(
 SELECT 
   ti_material_application_id -- '物料主表ID'
   ,tm_clas_id   -- '物料分类表ID'
   ,process_id    -- '流程编号'
   ,type      -- '申请物料类型'
   ,material_status    -- '物料状态'
   ,refence_material_no   -- '参考物料编码'
   ,tz_code     -- '特征码'
   ,material_no   -- '物料编码'
   ,name    -- '物料名称'
   ,groes    -- '规格型号'
   ,normt    -- '标准代号'
   ,description     -- '物料描述'
   ,eng_name   -- '英文名称'
   ,tm_plant_id    -- '工厂代码（工厂信息）'
   ,is_pdm     -- 'PLM物料视图'
   ,basic_unit    -- '基本单位'
   ,material_spec    -- '材料规格'
   ,erp_material_status -- 'ERP跨工厂状态'
   ,material_type   -- '物料类型'
   ,product_group    -- '产品组'
   ,ckg       -- '长*宽*高'
   ,net_weight     -- '净重'
   ,weight_unit    -- '重量单位'
   ,volume    -- '体积'
   ,volume_unit   -- '体积单位'
   ,consumable_part -- '是否易损件(Y代表是)'
   ,maintenance_part -- '是否保养件(Y代表是)'
   ,notice_part     -- '是否公告物料(Y代表是)'
   ,repair_package -- '是否维修包(Y代表是)'
   ,random_accessory_package -- '是否随机附件包(Y代表是)'
   ,zycz      -- '主要材质'
   ,zyt      -- '用途'
   ,zgzyl     -- '工作原理'
   ,purchase_tech_type  -- '采购技术要求'
   ,original_material_no -- '旧物料号(原物料编码)'
   ,create_time   -- '创建日期'
   ,tzk_code_cn    -- '特征码含义'
   ,reason      -- '申请理由'
   ,checkman      -- '审核人'
   ,doc_version    -- '版本号'
   ,snapshot_code   -- '快照号'
   ,operation   -- '操作'  
   ,product_level     -- '产品层次'
   ,category_code   -- '主体/总成'
   ,zeinr      -- '图号'
   ,gnms     -- '功能描述'
   ,is_ckd     -- 'CKD类型'
   ,initial_target_cost  -- '初始目标成本'
   ,is_config     -- '是否可配置物料(Y代表是)'
   ,pdm_material_status   -- 'PLM物料状态'
   ,pdm_product_lib   -- '版本号'
   ,oid     -- 'PLM一级文件夹'
   ,oid_second   -- 'PLM二级文件夹'
   ,hide    -- '是否隐藏'
   ,jsxy_id    -- '技术协议表的主键ID'
   ,creater_account     -- 'MDM申请人账号'
   ,creater_name      -- 'MDM申请人姓名'
   ,`size`       -- '外观尺寸'
FROM sany_dw.dwd_mdm_ti_material_application_df -- mini_size: ti_material_application_id 
) t1 

left join ( SELECT
    type_id
    ,entry_code
    ,ENTRY_NAME
   FROM sany_data.ods_mdm_ts_dict_entry_df -- mini_size: entry_code
   WHERE type_id = 11 
   and ds = '${IncStartDate}' 
   ) t3 
   on t1.type = t3.entry_code 

left join ( SELECT
    type_id
    ,entry_code
    ,ENTRY_NAME
    FROM sany_data.ods_mdm_ts_dict_entry_df -- mini_size: entry_code
    WHERE type_id = 13 
    and ds = '${IncStartDate}' 
   ) t4
  on t1.material_status = t4.entry_code 


-- left join (SELECT 
   -- matnr -- COMMENT string,物料编号
   -- ,werks -- COMMENT string,工厂
   -- ,beskz -- COMMENT string,采购类型
   -- ,lgpro -- COMMENT string,生产仓储地点
   -- ,lgfsb -- COMMENT string,外部采购仓储地点
   -- ,prctr -- COMMENT string,利润中心
   -- ,mmsta -- COMMENT string,特定工厂的物料状态
   -- ,sobsl -- COMMENT string,特殊采购类型
  -- FROM sany_dim.dim_sap_marc_df  -- mini_size: matnr
  -- ) tm 
  -- on t1.material_no = tm.matnr

-- left join (SELECT
  -- werks
  -- ,name1
  -- ,name2
  -- FROM sany_data.ods_sap_t001w_df -- mini_size = werks
  -- WHERE ds = '${IncStartDate}'
  -- ) t5 
  -- on tm.werks = t5.werks 

left join (SELECT 
   type_id
   ,entry_code
   ,ENTRY_NAME
   FROM sany_data.ods_mdm_ts_dict_entry_df -- mini_size = entry_code
   WHERE type_id = 33 
   and ds = '${IncStartDate}'
  ) t6
  on t1.erp_material_status = t6.entry_code

left join (SELECT 
    type_id
    ,entry_code
    ,ENTRY_NAME
    FROM sany_data.ods_mdm_ts_dict_entry_df -- mini_size = entry_code
    WHERE type_id = 14 
    and ds = '${IncStartDate}'
   ) t7
on t1.material_type = t7.entry_code

left join (SELECT 
    type_id
    ,entry_code
    ,ENTRY_NAME
   FROM sany_data.ods_mdm_ts_dict_entry_df -- mini_size = entry_code
   WHERE type_id = 25 
   and ds = '${IncStartDate}'
   ) t8
   on t1.product_group = t8.entry_code 

left join (SELECT 
    type_id
    ,entry_code
    ,ENTRY_NAME
   FROM sany_data.ods_mdm_ts_dict_entry_df -- mini_size = entry_code
   WHERE type_id = 20 
   and ds = '${IncStartDate}' 
   ) t9 
   on t1.volume_unit = t9.entry_code 

left join (SELECT
    id
    ,create_username
    ,create_time
    ,jsxy_name
    ,pdf_url
   FROM sany_data.ods_mdm_ti_jsxy_df -- mini_size = id
   WHERE ds = '${IncStartDate}' 
   ) t10
  on t1.jsxy_id = t10.id 

left join (SELECT
    id
    ,prod_level_no       --'产品层次代码'
    ,prod_level_desc    --'产品层次短名称'
    ,s_desc AS prod_level_s_desc  --'产品层次长名称'
    ,PROCESS_ID        --'流程ID'
    ,PROCESS_STATUS       --'流程状态'
    FROM sany_data.ods_mdm_tm_product_level_df -- mini_size: prod_level_no
    WHERE ds = '${IncStartDate}' 
   ) t11
  on t1.product_level = t11.id 

left join ( SELECT
   mandt  -- string COMMENT '物料编码'
   ,matnr  -- string COMMENT '物料编码'
   ,meins  -- string COMMENT '基本计量单位'
   ,mtart  -- string COMMENT '物料类型代码'
   ,matkl  -- string COMMENT '物料组代码'
   ,spart  -- string COMMENT '产品组代码'
   ,ntgew  -- string COMMENT '净重'
   ,gewei  -- string COMMENT '重量单位'
   ,groes  -- string COMMENT '大小/量纲'
   ,normt  -- string COMMENT '行业标准描述'
   ,zycz  -- string COMMENT '主要材质'
   ,wgcc  -- string COMMENT '外观尺寸'
   ,zmeabm  -- string COMMENT '尺寸单位'
   ,gnms  -- string COMMENT '功能描述'
  FROM sany_dim.dim_prd_mtrl_base_df -- mini_size: matnr
  WHERE ds = '${IncStartDate}' 
  ) t13
  on t1.material_no = t13.matnr

left join ( SELECT
   matkl
   ,wgbez
  FROM sany_data.ods_sap_t023t_df -- mini_size: matkl
  WHERE ds = '${IncStartDate}' 
  and spras = 1 
  ) t14
  on t13.matkl = t14.matkl 

-- left join ( SELECT
   -- werks
   -- ,sobsl
   -- ,ltext
  -- FROM sany_data.ods_sap_t460t_df -- mini_size: werks,sobsl
  -- WHERE spras = 1 
  -- and ds = '${IncStartDate}'
  -- ) t15
  -- on tm.werks = t15.werks
  -- and tm.sobsl = t15.sobsl 

left join( SELECT

   depth         --'层级'
   ,tm_clas_id
  ,type_no        --'分类编码'
  ,type_descs        --'分类描述'
  ,level1_id        --'一级分类ID'
  ,level1_desc       --'一级分类描述'
  ,level2_id        --'二级分类ID'
  ,level2_desc       --'二级分类描述'
  ,level3_id        --'三级分类ID'
  ,level3_desc       --'三级分类描述'
  ,level4_id        --'四级分类ID' 
  FROM sany_dim.dim_prd_mtrl_clss_df   --分类宽表 mini_size: tm_clas_id 
  WHERE ds = '${IncStartDate}'
  ) t12 
  on t1.tm_clas_id = t12.tm_clas_id

-- left join (SELECT 
   -- mtstb
   -- ,mmsta
  -- FROM sany_data.ods_sap_t141t_df -- mini_size: mmsta
  -- WHERE spras = 1 
  -- and ds = '${IncStartDate}'
  -- ) t16
  -- on tm.mmsta = t16.mmsta
-- ;
left join( SELECT
   id          
   ,code         -- '工厂代码'
  FROM sany_data.ods_mdm_tm_plant_df -- mini_size: id
  WHERE ds = '${IncStartDate}'
  ) tm 
  on t1.tm_plant_id = tm.id
;"""
    # parsed = sqlparse.parse(SQL)[0]
    # print(extract_insert_table_info(parsed))
    hqlparse = HqlParse(SQL)
    # print(hqlparse.tables)
    print("--- ---")
    print(hqlparse.insert_info)

    # parsed = sqlparse.parse(SQL)[0]
    #
    # # extract the parenthesis which holds column definitions
    # _, par = parsed.token_next_by(i=sqlparse.sql.Parenthesis)
    # columns = extract_definitions(par)
    #
    # for column in columns:
    #     print(' {name!s:12} {definition}'.format(
    #         name=column[0], definition=' '.join(str(t) for t in column[1:])))
    # print(extract_table_name(parsed))