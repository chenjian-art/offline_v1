use gmall;
set hive.exec.mode.local.auto = true;


DROP TABLE ads_zhuanhuan;
CREATE EXTERNAL TABLE ads_zhuanhuan
(
    `amount` decimal(16, 2) COMMENT '转化率'
) COMMENT '转化率'
    location "/warehouse/gmall/ads/ads_zhuanhuan"
    tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );

set hive.support.concurrency=false;
set hive.auto.convert.join= false;


select t.c / t2.s
from (select order_status,
             count(*) as c
      from dwd_order_info doi
               left join dwd_order_detail dod on doi.id = dod.order_id
      group by order_status) t
         left join (select count(*) as s
                    from dwd_order_info) t2;



DROP TABLE ads_fugolv;
CREATE EXTERNAL TABLE ads_fugolv
(
    `tm_id`  int COMMENT '品牌(冗余)',
    `amount` decimal(16, 2) COMMENT '复购率',
    `time`   string COMMENT '日期'
) COMMENT '转化率'
    location "/warehouse/gmall/ads/ads_fugolv"
    tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );
select * from ads_fugolv;

-- insert into ads_fugolv
select t.tm_id, t.c / t1.s*100, t1.time
from (select dsi.tm_id, count(*) as c
      from dwd_order_detail dod
               left join dwd_sku_info dsi on dod.sku_id = dsi.id
      group by dsi.tm_id
      having c >= 2) t
         left join (select substr(dt, 1, 7) as time, count(*) as s
                    from ods_order_detail
                    group by substr(dt, 1, 7)) t1;