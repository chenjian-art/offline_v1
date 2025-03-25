set hive.exec.mode.local.auto = true;
use gmall;


create table dws_order_info (
                                `user_id` int COMMENT '用户id',
                                `num` int COMMENT '下单次数',
                                `amount` decimal(16,2) COMMENT '下单金额'
)partitioned by (dt string)
location "/warehouse/gmall/dws/dws_order_info"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.autoformatting.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );

insert into dws_order_info partition (dt='2025-03-24')
select
    user_id,
    count(*),
    sum(order_price)
from dwd_order_detail
group by user_id having user_id is not null ;

select * from dws_order_info;