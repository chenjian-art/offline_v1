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







