----------------------------------------------
-- @ Output：db_name.table_name
-- @ Desc：订单表
-- @ Primary Key：
-- @ Author：DESKTOP-3MJUPJ1
-- @ Create Time：2021-6-30
-- @ Modify Time：(DESKTOP-3MJUPJ1，2021-6-30，创建表)
-- @ Comment：订单表
----------------------------------------------

CREATE TABLE IF NOT EXISTS XXXXX (
	`id`						string	COMMENT	'订单编号'
 	,`final_total_amount`		decimal(10,2)	COMMENT	'订单金额'
 	,`order_status`				string	COMMENT	'订单状态'
 	,`user_id`					string	COMMENT	'用户 id'
 	,`out_trade_no`				string	COMMENT	'支付流水号'
 	,`create_time`				string	COMMENT	'创建时间'
 	,`operate_time`				string	COMMENT	'操作时间'
 	,`province_id`				string	COMMENT	'省份 ID'
 	,`benefit_reduce_amount`	decimal(10,2)	COMMENT	'优惠金额'
 	,`original_total_amount`	decimal(10,2)	COMMENT	'原价金额'
 	,`feight_fee`				decimal(10,2)	COMMENT	'运费'
 	,`order_id`					string	COMMENT	'订单号'
 	,`sku_id`					string	COMMENT	'商品 id'
 	,`sku_name`					string	COMMENT	'商品名称'
 	,`order_price`				decimal(10,2)	COMMENT	'商品价格'
 	,`sku_num`					bigint	COMMENT	'商品数量'
 	,etl_time timestamp COMMENT 'etl_time'
)COMMENT ' '
-- 要不要分区？partitioned by (ds string)
ROW FORMAT DELIMITED STORED AS orc TBLPROPERTIES('orc.compression'='SNAPPY')
;
-- 分区检查
set hive.mapred.mode = strict;

insert overwrite table XXXXXX 分区？partition(ds = ' ' )
SELECT
	t1.`id`					--'订单号'
	,t1.`final_total_amount`	--'订单金额'
	,t1.`order_status`			--'订单状态'
	,t1.`user_id`				--'用户 id'
	,t1.`out_trade_no`			--'支付流水号'
	,t1.`create_time`			--'创建时间'
	,t1.`operate_time`			--'操作时间'
	,t1.`province_id`			--'省份 ID'
	,t1.`benefit_reduce_amount`	--'优惠金额'
	,t1.`original_total_amount`	--'原价金额'
	,t1.`feight_fee`			--'运费'
	,t2.`order_id`				--'订单号'
	,t2.`sku_id`				--'商品 id'
	,t2.`sku_name`				--'商品名称'
	,t2.`order_price`			--'商品价格'
	,t2.`sku_num`				--'商品数量'
 	,current_timestamp as etl_time
from
 (
         select
         `id`						--'订单号'
		 ,`final_total_amount`		--'订单金额'
		 ,`order_status`			--'订单状态'
		 ,`user_id`					--'用户 id'
		 ,`out_trade_no`			--'支付流水号'
		 ,`create_time`				--'创建时间'
		 ,`operate_time`			--'操作时间'
		 ,`province_id`				--'省份 ID'
		 ,`benefit_reduce_amount`	--'优惠金额'
		 ,`original_total_amount`	--'原价金额'
		 ,`feight_fee`				--'运费'
         from hive_ods.ods_order_info -- min_size:   comment:订单表
         -- where ds = ' '
         ) t1


LEFT JOIN(
         select
         `order_id`				--'订单号'
		 ,`sku_id`					--'商品 id'
		 ,`sku_name`				--'商品名称'
		 ,`order_price`				--'商品价格'
		 ,`sku_num`					--'商品数量'
         from hive_ods.ods_order_detail -- min_size:   comment: 订单详情表
         -- where ds = ' '
         ) t2
         ON
;