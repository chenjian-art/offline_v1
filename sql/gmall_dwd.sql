use gmall;


create table dwd_order_info(
                               `id` int COMMENT '编号',
                               `total_amount` decimal(16,2) COMMENT '总金额',
                               `order_status` string COMMENT '订单状态',
                               `user_id` int COMMENT '用户id',
                               `payment_way` string COMMENT '订单备注',
                               `out_trade_no` string COMMENT '订单交易编号（第三方支付用)',
                               `create_time` string COMMENT '创建时间',
                               `operate_time` string COMMENT '操作时间'
)COMMENT '订单表'
partitioned by (dt string)
location "/warehouse/gmall/dwd/dwd_order_info"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
select * from dwd_order_info;
insert into dwd_order_info partition (dt='2025-03-24')
select id, total_amount, order_status, user_id, payment_way, out_trade_no, create_time, operate_time
from ods_order_info;

DROP TABLE dwd_order_detail;
CREATE EXTERNAL TABLE dwd_order_detail(
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
location "/warehouse/gmall/dwd/dwd_order_detail"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
select * from dwd_order_detail;
insert into dwd_order_detail partition (dt='2025-03-24')
select id, order_id, user_id, sku_id, sku_name, img_url, order_price, sku_num, create_time
from ods_order_detail;

DROP TABLE dwd_user_info;
CREATE EXTERNAL TABLE dwd_user_info(
                                `id` int COMMENT '编号',
                                `name` string  COMMENT '用户姓名',
                                `birthday` string COMMENT '用户生日',
                                `gender` string  COMMENT '性别 M男,F女',
                                `email` string  COMMENT '邮箱',
                                `user_level` string  COMMENT '用户级别',
                                `create_time` string  COMMENT '创建时间'
) COMMENT '用户表'
partitioned by (dt string)
location "/warehouse/gmall/dwd/dwd_user_info"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
insert into dwd_user_info partition (dt='2025-03-24')
select id, name, birthday, gender, email, user_level, create_time
from ods_user_info;
select * from dwd_user_info;

DROP TABLE dwd_sku_info;
CREATE EXTERNAL TABLE dwd_sku_info(
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
location "/warehouse/gmall/dwd/dwd_sku_info"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
insert into dwd_sku_info partition (dt='2025-03-24')
select id, spu_id, price, sku_name, sku_desc, weight, tm_id, category3_id, create_time
from ods_sku_info;
select * from dwd_sku_info;