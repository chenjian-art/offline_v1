create databases gmall;

use gmall;

DROP TABLE IF EXISTS ods_activity_info;
CREATE EXTERNAL TABLE ods_activity_info(
                                           `id` STRING COMMENT '编号',
                                           `activity_name` STRING COMMENT '活动名称',
                                           `activity_type` STRING COMMENT '活动类型',
                                           `start_time` STRING COMMENT '开始时间',
                                           `end_time` STRING COMMENT '结束时间',
                                           `create_time` STRING COMMENT '创建时间'
) COMMENT '活动信息表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ods/ods_activity_info'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );

select * from ods_activity_info;
load data inpath '/origin_data/gmall/activity_info/2025-03-24' into table ods_activity_info partition (dt='2025-03-24');


DROP TABLE IF EXISTS ods_activity_order;
CREATE EXTERNAL TABLE ods_activity_order(
                                           `id` int COMMENT '编号',
                                           `activity_id` int COMMENT '活动id',
                                           `order_id` int COMMENT '订单编号',
                                           `create_time` STRING COMMENT '发生日期'
) COMMENT '活动信息表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ods/ods_activity_order'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );

select * from ods_activity_order;
load data inpath '/origin_data/gmall/activity_order/2025-03-24' into table ods_activity_order partition (dt='2025-03-24');

----

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
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ods/ods_activity_rule'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );


load data inpath '/origin_data/gmall/activity_rule/2025-03-24' into table ods_activity_rule partition (dt='2025-03-24');
select * from ods_activity_rule;
------
DROP TABLE IF EXISTS ods_base_category1;
CREATE EXTERNAL TABLE ods_base_category1(
                                            `id` STRING COMMENT 'id',
                                            `name` STRING COMMENT '名称'
) COMMENT '商品一级分类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ods/ods_base_category1'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );

load data inpath '/origin_data/gmall/base_category1/2025-03-24' into table ods_base_category1 partition (dt='2025-03-24');
select * from ods_base_category1;

-----
DROP TABLE IF EXISTS ods_base_category2;
CREATE EXTERNAL TABLE ods_base_category2(
                                            `id` STRING COMMENT ' id',
                                            `name` STRING COMMENT '名称',
                                            `category1_id` STRING COMMENT '一级品类 id'
) COMMENT '商品二级分类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ods/ods_base_category2/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );
load data inpath '/origin_data/gmall/base_category2/2025-03-24' into table ods_base_category2 partition (dt='2025-03-24');
select * from ods_base_category2;

------
DROP TABLE IF EXISTS ods_base_category3;
CREATE EXTERNAL TABLE ods_base_category3(
                                            `id` STRING COMMENT ' id',
                                            `name` STRING COMMENT '名称',
                                            `category2_id` STRING COMMENT '二级品类 id'
) COMMENT '商品三级分类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ods/ods_base_category3/' TBLPROPERTIES (
    'hive.exec.compress.output' = 'true',
    'mapreduce.output.fileoutputformat.compress' = 'true',
    'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath '/origin_data/gmall/base_category3/2025-03-24' into table ods_base_category3 partition (dt='2025-03-24');
select * from ods_base_category3;
---
DROP TABLE IF EXISTS ods_base_province;
CREATE EXTERNAL TABLE ods_base_province (
                                            `id` STRING COMMENT '编号',
                                            `name` STRING COMMENT '省份名称',
                                            `region_id` STRING COMMENT '地区 ID',
                                            `area_code` STRING COMMENT '地区编码',
                                            `iso_code` STRING COMMENT 'ISO-3166 编码，供可视化使用',
                                            `iso_3166_2` STRING COMMENT 'IOS-3166-2 编码，供可视化使用'
) COMMENT '省份表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ods/ods_base_province/'TBLPROPERTIES (
    'hive.exec.compress.output' = 'true',
    'mapreduce.output.fileoutputformat.compress' = 'true',
    'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath '/origin_data/gmall/base_province/2025-03-24' into table ods_base_province;
select * from ods_base_province;

---
DROP TABLE IF EXISTS ods_base_region;
CREATE EXTERNAL TABLE ods_base_region (
                                          `id` STRING COMMENT '编号',
                                          `region_name` STRING COMMENT '地区名称'
) COMMENT '地区表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ods/ods_base_region/'TBLPROPERTIES (
    'hive.exec.compress.output' = 'true',
    'mapreduce.output.fileoutputformat.compress' = 'true',
    'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath '/origin_data/gmall/base_region/2025-03-24' into table ods_base_region;
select * from ods_base_region;
----
DROP TABLE IF EXISTS ods_base_trademark;
CREATE EXTERNAL TABLE ods_base_trademark (
                                             `id` STRING COMMENT '编号',
                                             `tm_name` STRING COMMENT '品牌名称'
) COMMENT '品牌表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ods/ods_base_trademark/'TBLPROPERTIES (
    'hive.exec.compress.output' = 'true',
    'mapreduce.output.fileoutputformat.compress' = 'true',
    'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath '/origin_data/gmall/base_trademark/2025-03-24' into table ods_base_trademark partition (dt='2025-03-24');;
select * from ods_base_trademark;

--
DROP TABLE IF EXISTS ods_cart_info;
CREATE EXTERNAL TABLE ods_cart_info(
                                       `id` STRING COMMENT '编号',
                                       `user_id` STRING COMMENT '用户 id',
                                       `sku_id` STRING COMMENT 'skuid',
                                       `cart_price` DECIMAL(16,2) COMMENT '放入购物车时价格',
                                       `sku_num` BIGINT COMMENT '数量',
                                       `sku_name` STRING COMMENT 'sku 名称 (冗余)',
                                       `create_time` STRING COMMENT '创建时间',
                                       `operate_time` STRING COMMENT '修改时间',
                                       `is_ordered` STRING COMMENT '是否已经下单',
                                       `order_time` STRING COMMENT '下单时间',
                                       `source_type` STRING COMMENT '来源类型',
                                       `source_id` STRING COMMENT '来源编号'
) COMMENT '加购表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ods/ods_cart_info/' TBLPROPERTIES (
    'hive.exec.compress.output' = 'true',
    'mapreduce.output.fileoutputformat.compress' = 'true',
    'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath '/origin_data/gmall/cart_info/2025-03-24' into table ods_cart_info partition (dt='2025-03-24');;
select * from ods_cart_info;