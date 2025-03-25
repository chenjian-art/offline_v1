use gmall;

DROP TABLE ads_cjje;
CREATE EXTERNAL TABLE ads_cjje(
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
location "/warehouse/gmall/ads/ads_cjje"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
select

    sum(if(order_status="1001",dod.order_price,0)),
    sum(if(order_status="1002",dod.order_price,0)),
    sum(if(order_status="1003",dod.order_price,0)),
    sum(if(order_status="1004",dod.order_price,0)),
    sum(if(order_status="1005",dod.order_price,0))
from dwd_order_info doi
left join dwd_order_detail dod  on doi.id = dod.order_id;



select t1 / t2
from (select count(*)
      from dwd_order_detail) t1
         left join (select count(*) as cs
                    from dwd_sku_info dsi
                             left join dwd_order_detail dod on dsi.id = dod.sku_id
                    group by dsi.tm_id
                    having cs >= 2) t2;