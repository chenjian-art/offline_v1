use gmall;


DROP TABLE IF EXISTS ods_activity_info;
CREATE EXTERNAL TABLE ods_activity_info(
                                           `id` STRING COMMENT '编号',
                                           `activity_name` STRING COMMENT '活动名称',
                                           `activity_type` STRING COMMENT '活动类型',
                                           `start_time` STRING COMMENT '开始时间',
                                           `end_time` STRING COMMENT '结束时间',
                                           `create_time` STRING COMMENT '创建时间'
) COMMENT '活动表'
partitioned by (dt string)
row format delimited fields terminated by "\t"
location "/warehouse/gmall/ods/ods_activity_info"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath "/origin_data/gmall/activity_info" into table ods_activity_info partition (dt='2025-03-24');
select * from ods_activity_info;


DROP TABLE IF EXISTS ods_activity_order;
CREATE EXTERNAL TABLE ods_activity_order(
                                           `id` int COMMENT '编号',
                                           `activity_id` int COMMENT '活动id',
                                           `order_id` int COMMENT '订单编号',
                                           `create_time` STRING COMMENT '发生日期'
) COMMENT '活动与订单关联表'
partitioned by (dt string)
row format delimited fields terminated by "\t"
location "/warehouse/gmall/ods/ods_activity_order"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath "/origin_data/gmall/activity_order" into table ods_activity_order partition (dt = '2025-03-24');
select * from ods_activity_order;


DROP TABLE IF EXISTS ods_activity_rule;
CREATE EXTERNAL TABLE ods_activity_rule(
                                          `id` STRING COMMENT '编号',
                                           `activity_id` STRING COMMENT '活动 ID',
                                           `activity_type` STRING COMMENT '活动类型',
                                           `condition_amount` DECIMAL(16,2) COMMENT '满减金额',
                                           `condition_num` BIGINT COMMENT '满减件数',
                                           `benefit_amount` DECIMAL(16,2) COMMENT '优惠金额',
                                           `benefit_discount` DECIMAL(16,2) COMMENT '优惠折扣',
                                           `benefit_level` STRING COMMENT '优惠级别'
) COMMENT '活动规则表'
partitioned by (dt string)
row format delimited fields terminated by "\t"
location "/warehouse/gmall/ods/ods_activity_rule"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath "/origin_data/gmall/activity_rule" into table ods_activity_rule partition (dt = '2025-03-24');
select * from ods_activity_rule;


DROP TABLE IF EXISTS ods_activity_rule;
CREATE EXTERNAL TABLE ods_activity_rule(
                                          `id` STRING COMMENT '编号',
                                           `activity_id` STRING COMMENT '活动 ID',
                                           `activity_type` STRING COMMENT '活动类型',
                                           `condition_amount` DECIMAL(16,2) COMMENT '满减金额',
                                           `condition_num` BIGINT COMMENT '满减件数',
                                           `benefit_amount` DECIMAL(16,2) COMMENT '优惠金额',
                                           `benefit_discount` DECIMAL(16,2) COMMENT '优惠折扣',
                                           `benefit_level` STRING COMMENT '优惠级别'
) COMMENT '活动规则表'
partitioned by (dt string)
row format delimited fields terminated by "\t"
location "/warehouse/gmall/ods/ods_activity_rule"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath "/origin_data/gmall/activity_rule" into table ods_activity_rule partition (dt = '2025-03-24');
select * from ods_activity_rule;



DROP TABLE ods_order_info;
CREATE EXTERNAL TABLE ods_order_info(
                               `id` int COMMENT '编号',
                               `total_amount` decimal(16,2) COMMENT '总金额',
                               `order_status` string COMMENT '订单状态',
                               `user_id` int COMMENT '用户id',
                               `payment_way` string COMMENT '订单备注',
                               `out_trade_no` string COMMENT '订单交易编号（第三方支付用)',
                               `create_time` string COMMENT '创建时间',
                               `operate_time` string COMMENT '操作时间'
) COMMENT '订单表'
partitioned by (dt string)
row format delimited fields terminated by "\t"
location "/warehouse/gmall/ods/ods_order_info"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );

load data inpath "/origin_data/gmall/order_info" into table ods_order_info partition (dt = '2025-03-24');
select *
from ods_order_info;


DROP TABLE ods_order_detail;
CREATE EXTERNAL TABLE ods_order_detail(
                                `id` int COMMENT '编号',
                                `order_id` int  COMMENT '订单编号',
                                `user_id` int COMMENT '用户id',
                                `sku_id` int  COMMENT 'sku_id',
                                `sku_name` string  COMMENT 'sku名称（冗余)',
                                `img_url` string  COMMENT '图片名称（冗余)',
                                `order_price` decimal(10,2)  COMMENT '购买价格(下单时sku价格）',
                                `sku_num` string  COMMENT '购买个数',
                                `create_time` string  COMMENT '创建时间'
) COMMENT '订单明细表'
partitioned by (dt string)
row format delimited fields terminated by "\t"
location "/warehouse/gmall/ods/ods_order_detail"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath "/origin_data/gmall/order_detail" into table ods_order_detail partition (dt = '2025-03-24');
select * from ods_order_detail;

DROP TABLE ods_user_info;
CREATE EXTERNAL TABLE ods_user_info(
                                `id` int COMMENT '编号',
                                `name` string  COMMENT '用户姓名',
                                `birthday` string COMMENT '用户生日',
                                `gender` string  COMMENT '性别 M男,F女',
                                `email` string  COMMENT '邮箱',
                                `user_level` string  COMMENT '用户级别',
                                `create_time` string  COMMENT '创建时间'
) COMMENT '用户表'
partitioned by (dt string)
row format delimited fields terminated by "\t"
location "/warehouse/gmall/ods/ods_user_info"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath "/origin_data/gmall/user_info" into table ods_user_info partition (dt = '2025-03-24');
select * from ods_user_info;

DROP TABLE ods_sku_info;
CREATE EXTERNAL TABLE ods_sku_info(
                             `id` int COMMENT 'skuid(itemID)',
                             `spu_id` int COMMENT 'spuid',
                             `price` decimal(10,0) COMMENT '价格',
                             `sku_name` string COMMENT 'sku名称',
                             `sku_desc` string COMMENT '商品规格描述',
                             `weight` decimal(10,2)  COMMENT '重量',
                             `tm_id` int COMMENT '品牌(冗余)',
                             `category3_id` int COMMENT '三级分类id（冗余)',
                             `create_time` string COMMENT '创建时间'
) COMMENT '用户表'
partitioned by (dt string)
row format delimited fields terminated by "\t"
location "/warehouse/gmall/ods/ods_sku_info"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath "/origin_data/gmall/sku_info" into table ods_sku_info partition (dt = '2025-03-24');
select * from ods_sku_info;