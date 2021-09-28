create external table hive_ods.ods_order_info_df (
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
create external table hive_ods.ods_order_detail_df(
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
