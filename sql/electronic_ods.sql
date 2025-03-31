create database electronic;

use electronic;


drop table ods_price_power;
create external table ods_price_power(
  `id` int COMMENT 'ID',
  `product_id` int COMMENT '商品ID',
  `price_power_star` int COMMENT '价格力星级(1-5星)',
  `inclusive_after_price` decimal(10,2) COMMENT '普惠券后价',
  `price_power_level` string COMMENT '价格力等级',
  `create_time` string COMMENT '记录创建时间'
)COMMENT '商品价格力评估表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/electronic/ods/ods_price_power"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);
load data inpath "/origin_data/electronic/price_power/20250330" into table ods_price_power partition (ds = '20250330');
select * from ods_price_power;


drop table ods_product_info;
create external table ods_product_info(
  `product_id` int COMMENT '商品ID，自增主键',
  `product_name` string COMMENT '商品名称',
  `category_id` int COMMENT '类目ID',
  `category_name` string COMMENT '类目名称',
  `current_inventory` int COMMENT '当前库存数量',
  `create_time` string COMMENT '记录创建时间'
)COMMENT '商品基础信息表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/electronic/ods/ods_product_info"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);
load data inpath "/origin_data/electronic/product_info/20250330" into table ods_product_info partition (ds = '20250330');
select * from ods_product_info;


drop table ods_sales_detail;
create external table ods_sales_detail(
  `id` int COMMENT '自增主键ID',
  `order_id` int COMMENT '订单ID',
  `product_id` int COMMENT '商品ID',
  `sales_amount` decimal(10,2) COMMENT '销售金额',
  `sales_volume` int COMMENT '销售数量',
  `paying_buyer_id` int COMMENT '支付买家ID',
  `paying_items` int COMMENT '支付商品件数',
  `payment_time` string COMMENT '支付时间',
  `create_time` string COMMENT '记录创建时间'
)COMMENT '销售明细记录表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/electronic/ods/ods_sales_detail"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);
load data inpath "/origin_data/electronic/sales_detail/20250330" into table ods_sales_detail partition (ds = '20250330');
select * from ods_sales_detail;


drop table ods_search_term;
create external table ods_search_term(
  `id` int COMMENT '自增主键ID',
  `product_id` int COMMENT '商品ID',
  `search_term` string COMMENT '搜索关键词',
  `search_count` int COMMENT '搜索次数',
  `create_time` string COMMENT '记录创建时间'
)COMMENT '商品搜索词表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/electronic/ods/ods_search_term"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);
load data inpath "/origin_data/electronic/search_term/20250330" into table ods_search_term partition (ds = '20250330');
select * from ods_search_term;


drop table ods_traffic_detail;
create external table ods_traffic_detail(
  `id` int COMMENT '自增主键ID',
  `product_id` int COMMENT '商品ID',
  `visitor_id` int COMMENT '访客ID',
  `visit_time` string COMMENT '访问时间',
  `traffic_source` string COMMENT '流量来源',
  `create_time` string COMMENT '记录创建时间'
)COMMENT '商品流量明细表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/electronic/ods/ods_traffic_detail"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);
load data inpath "/origin_data/electronic/traffic_detail/20250330" into table ods_traffic_detail partition (ds = '20250330');
select * from ods_traffic_detail;