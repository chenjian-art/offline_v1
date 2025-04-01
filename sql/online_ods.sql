create database online;


use online;


drop table ods_coupons;
create external table ods_coupons(
  `id` int COMMENT '记录自增唯一标识',
  `product_id` int COMMENT '关联的商品 ID',
  `coupon_date` string COMMENT '优惠券使用日期',
  `original_price` decimal(10,2) COMMENT '商品原价',
  `coupon_discount` decimal(10,2) COMMENT '优惠券折扣金额',
  `coupon_discounted_price` decimal(10,2) COMMENT '使用优惠券后的商品价格'
)COMMENT '优惠券使用表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/online/ods/ods_coupons"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);
load data inpath "/origin_data/online/coupons/20250330" into table ods_coupons partition (ds = '20250330');
select * from ods_coupons;


drop table ods_inventory;
create external table ods_inventory(
  `id` int COMMENT '记录自增唯一标识',
  `product_id` int COMMENT '关联的商品 ID',
  `inventory_date` string COMMENT '库存数据统计日期',
  `current_inventory` int COMMENT '当前商品库存数量'
)COMMENT '库存表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/online/ods/ods_inventory"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);
load data inpath "/origin_data/online/inventory/20250330" into table ods_inventory partition (ds = '20250330');
select * from ods_inventory;


drop table ods_orders;
create external table ods_orders(
  `order_id` int COMMENT '订单自增唯一标识',
  `product_id` int COMMENT '关联的商品 ID',
  `order_date` string COMMENT '订单生成日期',
  `order_amount` decimal(10,2) COMMENT '订单金额',
  `order_quantity` int COMMENT '订单中商品的数量',
  `payment_status` string COMMENT '订单支付状态'
)COMMENT '订单表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/online/ods/ods_orders"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);
load data inpath "/origin_data/online/orders/20250330" into table ods_orders partition (ds = '20250330');
select * from ods_orders;


drop table ods_price_force;
create external table ods_price_force(
  `id` int COMMENT '记录自增唯一标识',
  `product_id` int COMMENT '关联的商品 ID',
  `price_force_date` string COMMENT '价格力数据统计日期',
  `price_force_star` int COMMENT '商品价格力星级',
  `price_force_level` string COMMENT '商品价格力等级',
  `is_price_force_warning` int COMMENT '是否为价格力预警商品',
  `is_product_force_warning` int COMMENT '是否为商品力预警商品'
)COMMENT '价格力表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/online/ods/ods_price_force"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);
load data inpath "/origin_data/online/price_force/20250330" into table ods_price_force partition (ds = '20250330');
select * from ods_price_force;


drop table ods_products;
create external table ods_products(
  `product_id` int COMMENT '商品自增唯一标识',
  `product_name` string COMMENT '商品名称',
  `category` string COMMENT '商品所属类别',
  `brand` string COMMENT '商品品牌'
)COMMENT '商品信息'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/online/ods/ods_products"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);
load data inpath "/origin_data/online/products/20250330" into table ods_products partition (ds = '20250330');
select * from ods_products;


drop table ods_search_words;
create external table ods_search_words(
  `id` int COMMENT '记录自增唯一标识',
  `product_id` int COMMENT '关联的商品 ID',
  `search_date` string COMMENT '搜索行为发生日期',
  `search_word` string COMMENT '用户搜索使用的关键词'
)COMMENT '关键词'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/online/ods/ods_search_words"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);
load data inpath "/origin_data/online/search_words/20250330" into table ods_search_words partition (ds = '20250330');
select * from ods_search_words;


drop table ods_sku;
create external table ods_sku(
  `sku_id` int COMMENT 'SKU 自增唯一标识',
  `product_id` int COMMENT '关联的商品 ID',
  `sku_sales_date` string COMMENT 'SKU 销售日期',
  `sku_payment_quantity` int COMMENT 'SKU 支付数量'
)COMMENT 'SKU表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/online/ods/ods_sku"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);
load data inpath "/origin_data/online/sku/20250330" into table ods_sku partition (ds = '20250330');
select * from ods_sku;


drop table ods_traffic;
create external table ods_traffic(
  `id` int COMMENT '记录自增唯一标识',
  `product_id` int COMMENT '关联的商品 ID',
  `traffic_date` string COMMENT '流量数据统计日期',
  `traffic_source` string COMMENT '流量来源渠道',
  `visitors` int COMMENT '访客数量',
  `payment_buyers` int COMMENT '支付买家数量'
)COMMENT '流量信息'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/online/ods/ods_traffic"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);
load data inpath "/origin_data/online/traffic/20250330" into table ods_traffic partition (ds = '20250330');
select * from ods_traffic;