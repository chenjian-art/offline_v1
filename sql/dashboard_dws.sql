use dashboard;
set hive.support.concurrency=false;
set hive.auto.convert.join= false;


drop table dws_core;
CREATE EXTERNAL TABLE dws_core (
     product_id INT COMMENT '商品ID',
    dt STRING COMMENT '统计日期',
    product_sales_amount DECIMAL(10, 2) COMMENT '商品销售额',
    product_sales_quantity INT COMMENT '商品销售量',
    product_avg_price DECIMAL(18, 2) COMMENT '商品平均价格',
    create_time TIMESTAMP COMMENT '数据创建时间'
) COMMENT '商品销售指标'
STORED AS orc
LOCATION "/warehouse/dashboard/dws/dws_core"
TBLPROPERTIES ('orc.compress'='SNAPPY');
select * from dws_core;
-- 插入数据到dws_core表
INSERT INTO TABLE dws_core
SELECT
    op.item_id AS product_id,
    '20250401' AS dt, -- 这里假设统计日期固定为20250401，实际使用中可根据需求动态获取
    SUM(od.price * od.quantity) AS product_sales_amount,
    SUM(od.quantity) AS product_sales_quantity,
    CASE
        WHEN SUM(od.quantity) = 0 THEN 0
        ELSE SUM(od.price * od.quantity) / SUM(od.quantity)
    END AS product_avg_price,
    current_timestamp AS create_time
FROM
    ods_order_detail od
JOIN
    ods_product op ON od.item_id = op.item_id
GROUP BY
    op.item_id;


drop table dws_traffic;
CREATE EXTERNAL TABLE dws_traffic (
    product_id INT COMMENT '商品ID',
    dt STRING COMMENT '统计日期',
    product_click_count INT COMMENT '商品点击量',
    channel1_flow_ratio DECIMAL(5, 4) COMMENT '渠道流量占比',
    root_word1_conversion_rate DECIMAL(5, 4) COMMENT '词根引流转化率',
    search_term1_flow_ratio DECIMAL(5, 4) COMMENT '搜索词引流占比',
    update_time TIMESTAMP COMMENT '数据更新时间'
) COMMENT '商品流量与转化指标'
STORED AS orc
LOCATION "/warehouse/dashboard/dws/dws_traffic"
TBLPROPERTIES ('orc.compress'='SNAPPY');
select * from dws_traffic;
INSERT INTO TABLE dws_traffic
SELECT
    ub.item_id AS product_id,
    '20250401' AS dt,
    COUNT(CASE WHEN ub.action = 'click' THEN 1 END) AS product_click_count,
    (SUM(CASE WHEN cf.channel_id = 1 THEN ub.log_id ELSE 0 END) / COUNT(ub.log_id)) AS channel1_flow_ratio,
    (SUM(CASE WHEN oi.order_id IS NOT NULL THEN 1 ELSE 0 END) / SUM(CASE WHEN st.search_term IS NOT NULL THEN 1 ELSE 0 END)) AS root_word1_conversion_rate,
    (SUM(CASE WHEN st.search_term IS NOT NULL THEN 1 ELSE 0 END) / COUNT(ub.log_id)) AS search_term1_flow_ratio,
    CURRENT_TIMESTAMP AS update_time
FROM
    ods_user_behavior ub
LEFT JOIN
    ods_channel_flow cf ON ub.channel_id = cf.channel_id
LEFT JOIN
    ods_order_info oi ON ub.user_id = oi.user_id
LEFT JOIN
    (SELECT DISTINCT search_term FROM ods_user_behavior) st ON ub.search_term = st.search_term
WHERE
    ub.ds = '20250401'
GROUP BY
    ub.item_id;

CREATE EXTERNAL TABLE dws_customer (
   product_id INT COMMENT '商品ID',
    dt STRING COMMENT '统计日期',
    new_customer_discount_participation_rate DECIMAL(5, 4) COMMENT '新客折扣参与率',
    age_group1_search_crowd_ratio DECIMAL(5, 4) COMMENT '年龄区间占比',
    payment_crowd_ratio DECIMAL(5, 4) COMMENT '支付人群性比例',
    data_version INT COMMENT '数据版本号'
) COMMENT '商品顾客洞察指标'
STORED AS orc
LOCATION "/warehouse/dashboard/dws/dws_customer"
TBLPROPERTIES ('orc.compress'='SNAPPY');
select * from dws_customer;
INSERT overwrite TABLE dws_customer
SELECT
    op.item_id AS product_id,
    '20250401' AS dt, -- 假设统计日期，实际应按需动态获取
    -- 新客折扣参与率计算
    CASE
        WHEN COUNT(DISTINCT ou.user_id) = 0 THEN 0
        ELSE COUNT(DISTINCT CASE WHEN ou.is_new_customer = 1 AND ooi.promotion_id IS NOT NULL THEN ou.user_id END) / COUNT(DISTINCT ou.user_id)
    END AS new_customer_discount_participation_rate,
    -- 年龄区间占比（假设年龄区间1为20 - 30岁）
    CASE
        WHEN COUNT(DISTINCT ou.user_id) = 0 THEN 0
        ELSE COUNT(DISTINCT CASE WHEN ou.age BETWEEN 20 AND 30 THEN ou.user_id END) / COUNT(DISTINCT ou.user_id)
    END AS age_group1_search_crowd_ratio,
    -- 支付人群性比例
    CASE
        WHEN COUNT(DISTINCT CASE WHEN ou.gender = 'M' AND ooi.payment_method IS NOT NULL THEN ou.user_id END) = 0 THEN 0
        WHEN COUNT(DISTINCT CASE WHEN ou.gender = 'F' AND ooi.payment_method IS NOT NULL THEN ou.user_id END) = 0 THEN NULL
        ELSE COUNT(DISTINCT CASE WHEN ou.gender = 'M' AND ooi.payment_method IS NOT NULL THEN ou.user_id END) / COUNT(DISTINCT CASE WHEN ou.gender = 'F' AND ooi.payment_method IS NOT NULL THEN ou.user_id END)
    END AS payment_crowd_ratio,
    1 AS data_version -- 假设数据版本号为1，实际按需设定
FROM
    ods_product op
JOIN
    ods_order_info ooi ON op.item_id = ooi.promotion_id -- 假设ods_order_info有product_id字段关联商品，若实际无则需调整关联逻辑
JOIN
    ods_user_info ou ON ooi.user_id = ou.user_id
GROUP BY
    op.item_id;


CREATE EXTERNAL TABLE dws_attribute (
    product_id INT COMMENT '商品ID',
    dt STRING COMMENT '统计日期',
    sales_quantity_ratio DECIMAL(5, 4) COMMENT '销售量占比',
    attribute_combination1_sales_ratio DECIMAL(5, 4) COMMENT '属性销售占比',
    attribute_stat_flag STRING COMMENT '属性统计标记'
) COMMENT '商品属性销售指标'
STORED AS orc
LOCATION "/warehouse/dashboard/dws/dws_attribute"
TBLPROPERTIES ('orc.compress'='SNAPPY');
select * from dws_attribute;
INSERT OVERWRITE TABLE dws_attribute
SELECT
    t.item_id AS product_id,
    '20250401' AS dt,
    t.quantity / total_quantity AS sales_quantity_ratio,
    t.quantity / attribute_quantity AS attribute_combination1_sales_ratio,
    CASE
        WHEN t.quantity > 0 THEN 'Y'
        ELSE 'N'
    END AS attribute_stat_flag
FROM (
    SELECT
        item_id,
        attribute_combination,
        SUM(quantity) AS quantity
    FROM ods_order_detail
    WHERE ds = '20250401'
    GROUP BY item_id,attribute_combination
) t
JOIN (
    SELECT
        SUM(quantity) AS total_quantity
    FROM ods_order_detail
    WHERE ds = '20250401'
) total ON 1 = 1
JOIN (
    SELECT
        item_id,
        attribute_combination,
        SUM(quantity) AS attribute_quantity
    FROM
        ods_order_detail
    WHERE
        ds = '20250401'
    GROUP BY
        item_id,
        attribute_combination
) attribute ON t.item_id = attribute.item_id AND t.attribute_combination = attribute.attribute_combination;


drop table dws_review;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_review (
    product_id INT COMMENT '商品ID',
    dt STRING COMMENT '统计日期',
    new_customer_discount_participation_rate DECIMAL(5, 4) COMMENT '新客折扣参与率',
    age_group1_search_crowd_ratio DECIMAL(5, 4) COMMENT '年龄区间占比',
    payment_crowd_ratio DECIMAL(5, 4) COMMENT '支付人群性比例'
) COMMENT '商品评价分析指标'
STORED AS orc
LOCATION "/warehouse/dashboard/dws/dws_review"
TBLPROPERTIES ('orc.compress'='SNAPPY');
select * from dws_review;
-- 插入数据到 dws_review 表
INSERT OVERWRITE TABLE dws_review
SELECT
    p.item_id AS product_id,
    '20250401' AS dt,
    -- 计算新客折扣参与率
    (SUM(CASE WHEN ui.is_new_customer = 1 AND oi.promotion_id IS NOT NULL THEN 1 ELSE 0 END) /(CASE WHEN SUM(CASE WHEN ui.is_new_customer = 1 THEN 1 ELSE 0 END) = 0 THEN 1 ELSE SUM(CASE WHEN ui.is_new_customer = 1 THEN 1 ELSE 0 END) END)) AS new_customer_discount_participation_rate,
    -- 计算年龄区间占比（假设年龄区间 1 为 18 - 25 岁）
    (SUM(CASE WHEN ui.age BETWEEN 18 AND 25 AND ub.action = 'search' THEN 1 ELSE 0 END) /
    (CASE WHEN SUM(CASE WHEN ub.action = 'search' THEN 1 ELSE 0 END) = 0 THEN 1 ELSE SUM(CASE WHEN ub.action = 'search' THEN 1 ELSE 0 END) END)) AS age_group1_search_crowd_ratio,
    -- 计算支付人群性比例（假设男性为 1，女性为 0）
    (SUM(CASE WHEN ui.gender = '1' AND oi.payment_method IS NOT NULL THEN 1 ELSE 0 END) /
    (CASE WHEN SUM(CASE WHEN oi.payment_method IS NOT NULL THEN 1 ELSE 0 END) = 0 THEN 1 ELSE SUM(CASE WHEN oi.payment_method IS NOT NULL THEN 1 ELSE 0 END) END)) AS payment_crowd_ratio
FROM
    ods_product p
LEFT JOIN
    ods_order_detail od ON p.item_id = od.item_id
LEFT JOIN
    ods_order_info oi ON od.order_id = oi.order_id
LEFT JOIN
    ods_user_info ui ON oi.user_id = ui.user_id
LEFT JOIN
    ods_user_behavior ub ON oi.user_id = ub.user_id AND p.item_id = ub.item_id
LEFT JOIN
    ods_promotion pr ON oi.promotion_id = pr.promotion_id
WHERE
    p.ds = '20250401'
    AND od.ds = '20250401'
    AND oi.ds = '20250401'
    AND ui.ds = '20250401'
    AND ub.ds = '20250401'
    AND pr.ds = '20250401'
GROUP BY
    p.item_id;
