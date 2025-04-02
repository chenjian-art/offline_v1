create database dashboard;


use dashboard;

drop table ods_channel_flow;
CREATE EXTERNAL TABLE ods_channel_flow (
    `channel_id` int COMMENT '渠道id',
    `channel_name` string COMMENT '渠道名称',
    `visit_count` int COMMENT '访问量',
    `conversion_count` int COMMENT '转化量'
)COMMENT '渠道流量统计表'
partitioned by (ds string)
row format delimited fields terminated by "\t"
STORED AS ORC
LOCATION '/warehouse/dashboard/ods/ods_channel_flow'
tblproperties ('orc.compress'='SNAPPY');
load data inpath "/origin_data/dashboard/channel_flow" into table ods_channel_flow partition (ds = '20250401');


drop table ods_live_stream;
CREATE EXTERNAL TABLE ods_live_stream (
    `live_id` int COMMENT '直播ID',
    `watch_duration` int COMMENT '观看时长(秒)',
    `interaction_count` int COMMENT '互动次数',
    `start_time` string COMMENT '开始时间',
    `end_time` string COMMENT '结束时间'
)COMMENT '直播数据表'
partitioned by (ds string)
row format delimited fields terminated by "\t"
STORED AS ORC
LOCATION '/warehouse/dashboard/ods/ods_live_stream'
tblproperties ('orc.compress'='SNAPPY');
load data inpath "/origin_data/dashboard/live_stream" into table ods_live_stream partition (ds = '20250401');



drop table ods_order_detail;
CREATE EXTERNAL TABLE ods_order_detail (
    `detail_id` int COMMENT '明细ID',
    `order_id` int COMMENT '订单ID',
    `item_id` int COMMENT '商品ID',
    `sku_id` int COMMENT 'SKUID',
    `quantity` int COMMENT '购买数量',
    `price` decimal(10,2) COMMENT '单价',
    `attribute_combination` string COMMENT '属性组合'
)COMMENT '订单明细表'
partitioned by (ds string)
row format delimited fields terminated by "\t"
STORED AS ORC
LOCATION '/warehouse/dashboard/ods/ods_order_detail'
tblproperties ('orc.compress'='SNAPPY');
load data inpath "/origin_data/dashboard/order_detail" into table ods_order_detail partition (ds = '20250401');


drop table ods_order_info;
CREATE EXTERNAL TABLE ods_order_info (
  `order_id` int COMMENT '订单ID',
  `user_id` int COMMENT '用户ID',
  `order_time` string COMMENT '下单时间',
  `total_amount` decimal(10,2) COMMENT '订单总额',
  `payment_method` string COMMENT '支付方式',
  `promotion_id` int COMMENT '促销活动ID'
)COMMENT '订单主表'
partitioned by (ds string)
row format delimited fields terminated by "\t"
STORED AS ORC
LOCATION '/warehouse/dashboard/ods/ods_order_info'
tblproperties ('orc.compress'='SNAPPY');
load data inpath "/origin_data/dashboard/order_info" into table ods_order_info partition (ds = '20250401');


drop table ods_product;
CREATE EXTERNAL TABLE ods_product (
  `item_id` int COMMENT '商品ID',
  `category_id` int COMMENT '类目ID',
  `base_price` decimal(10,2) COMMENT '基础价格',
  `current_price` decimal(10,2) COMMENT '当前售价',
  `stock` int COMMENT '库存数量',
  `title` string COMMENT '商品标题'
)COMMENT '商品基础信息表'
partitioned by (ds string)
row format delimited fields terminated by "\t"
STORED AS ORC
LOCATION '/warehouse/dashboard/ods/ods_product'
tblproperties ('orc.compress'='SNAPPY');
load data inpath "/origin_data/dashboard/product" into table ods_product partition (ds = '20250401');


drop table ods_promotion;
CREATE EXTERNAL TABLE ods_promotion (
  `promotion_id` int COMMENT '活动ID',
  `discount_type` string COMMENT '优惠类型',
  `start_time` string COMMENT '开始时间',
  `end_time` string COMMENT '结束时间'
)COMMENT '促销活动表'
partitioned by (ds string)
row format delimited fields terminated by "\t"
STORED AS ORC
LOCATION '/warehouse/dashboard/ods/ods_promotion'
tblproperties ('orc.compress'='SNAPPY');
load data inpath "/origin_data/dashboard/promotion" into table ods_promotion partition (ds = '20250401');


drop table ods_review;
CREATE EXTERNAL TABLE ods_review (
  `review_id` int COMMENT '评价ID',
  `item_id` int COMMENT '商品ID',
  `user_id` int COMMENT '用户ID',
  `rating` int COMMENT '评分(1 - 5)',
  `review_time` string COMMENT '评价时间',
  `content` string COMMENT '评价内容'
)COMMENT '商品评价表'
partitioned by (ds string)
row format delimited fields terminated by "\t"
STORED AS ORC
LOCATION '/warehouse/dashboard/ods/ods_review'
tblproperties ('orc.compress'='SNAPPY');
load data inpath "/origin_data/dashboard/review" into table ods_review partition (ds = '20250401');


drop table ods_short_video;
CREATE EXTERNAL TABLE ods_short_video (
  `video_id` int COMMENT '视频ID',
  `like_count` int COMMENT '点赞数',
  `share_count` int COMMENT '分享数',
  `post_time` string COMMENT '发布时间'
)COMMENT '短视频互动表'
partitioned by (ds string)
row format delimited fields terminated by "\t"
STORED AS ORC
LOCATION '/warehouse/dashboard/ods/ods_short_video'
tblproperties ('orc.compress'='SNAPPY');
load data inpath "/origin_data/dashboard/short_video" into table ods_short_video partition (ds = '20250401');


drop table ods_user_behavior;
CREATE EXTERNAL TABLE ods_user_behavior (
  `log_id` int COMMENT '日志ID',
  `user_id` int COMMENT '用户ID',
  `item_id` int COMMENT '商品ID',
  `action` string COMMENT '行为类型',
  `channel` string COMMENT '流量来源',
  `search_term` string COMMENT '搜索词',
  `live_stream_id` int COMMENT '直播ID',
  `short_video_id` int COMMENT '短视频ID',
  `channel_id` int COMMENT '渠道id',
  `event_time` string COMMENT '事件时间'
)COMMENT '用户行为原始表'
partitioned by (ds string)
row format delimited fields terminated by "\t"
STORED AS ORC
LOCATION '/warehouse/dashboard/ods/ods_user_behavior'
tblproperties ('orc.compress'='SNAPPY');
load data inpath "/origin_data/dashboard/user_behavior" into table ods_user_behavior partition (ds = '20250401');


drop table ods_user_info;
CREATE EXTERNAL TABLE ods_user_info (
  `user_id` int COMMENT '用户ID',
  `gender` string COMMENT '性别',
  `age` int COMMENT '年龄',
  `is_new_customer` int COMMENT '是否新客',
  `register_date` string COMMENT '注册日期'
)COMMENT '用户信息表'
partitioned by (ds string)
row format delimited fields terminated by "\t"
STORED AS ORC
LOCATION '/warehouse/dashboard/ods/ods_user_info'
tblproperties ('orc.compress'='SNAPPY');
load data inpath "/origin_data/dashboard/user_info" into table ods_user_info partition (ds = '20250401');