use online;


drop table dws_order_metrics;
CREATE external TABLE dws_order_metrics(
    product_id              int COMMENT '商品ID',
    time_dimension          string COMMENT '时间维度，1天、7天、30天',
    sales_amount            decimal(10, 2) COMMENT '销售额',
    sales_quantity          int COMMENT '销量',
    order_count             int COMMENT '订单数量',
    avg_order_amount        decimal(10, 2) COMMENT '平均订单金额'
)COMMENT"销售与订单相关汇总表"
location "/warehouse/online/dws/dws_order_metrics"
tblproperties (
'hive.exec.compress.output' = 'true',
'mapreduce.output.fileoutputformat.compress' = 'true',
'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);
select * from dws_order_metrics;
INSERT INTO TABLE dws_order_metrics
SELECT
    o.product_id,
    CASE
        WHEN MIN(o.order_date) = MAX(o.order_date) THEN '1天'
        WHEN DATEDIFF(MAX(o.order_date), MIN(o.order_date)) <= 6 THEN '7天'
        WHEN DATEDIFF(MAX(o.order_date), MIN(o.order_date)) <= 29 THEN '30天'
        ELSE NULL
    END AS time_dimension,
    SUM(o.order_amount) AS sales_amount,
    SUM(o.order_quantity) AS sales_quantity,
    COUNT(DISTINCT o.order_id) AS order_count,
    AVG(o.order_amount) AS avg_order_amount
FROM
    ods_orders o
WHERE
    o.order_date BETWEEN DATE_SUB(CURRENT_DATE, 29) AND CURRENT_DATE
GROUP BY
    o.product_id;


drop table dws_search_metrics;
CREATE external TABLE dws_search_metrics(
    product_id              int COMMENT '商品ID',
    time_dimension          string COMMENT '时间维度，1天、7天、30天',
    total_visitors          int COMMENT '总访客数',
    top_traffic_source      string COMMENT 'TOP流量来源渠道',
    top_traffic_source_visitors int COMMENT 'TOP流量来源的访客数',
    top_search_word         string COMMENT 'Top搜索词'
) COMMENT '流量与搜索相关汇总表'
LOCATION '/warehouse/online/dws/dws_search_metrics'
tblproperties (
'hive.exec.compress.output' = 'true',
'mapreduce.output.fileoutputformat.compress' = 'true',
'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);
select * from dws_search_metrics;
-- 插入数据
INSERT INTO TABLE dws_search_metrics
SELECT
    t.product_id,
    CASE
        WHEN MIN(t.traffic_date) = MAX(t.traffic_date) THEN '1天'
        WHEN DATEDIFF(MAX(t.traffic_date), MIN(t.traffic_date)) <= 6 THEN '7天'
        WHEN DATEDIFF(MAX(t.traffic_date), MIN(t.traffic_date)) <= 29 THEN '30天'
        ELSE NULL
    END AS time_dimension,
    SUM(t.visitors) AS total_visitors,
    t1.traffic_source AS top_traffic_source,
    t1.visitors AS top_traffic_source_visitors,
    sw1.search_word AS top_search_word
FROM
    ods_traffic t
-- 取流量来源访客数及转化率时，先筛选出当天流量数据并按访客数降序排名取第1来源
JOIN (
    SELECT product_id, traffic_source, visitors,
           ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY visitors DESC) AS rank
    FROM ods_traffic
    WHERE traffic_date BETWEEN DATE_SUB(CURRENT_DATE, 29) AND CURRENT_DATE
) t1 ON t.product_id = t1.product_id AND t1.rank = 1
-- 取搜索词时，先筛选出当天搜索数据并按搜索次数降序排名取第1词
JOIN (
    SELECT product_id, search_word, COUNT(*) AS search_count,
           ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY COUNT(*) DESC) AS rank
    FROM ods_search_words
    WHERE search_date BETWEEN DATE_SUB(CURRENT_DATE, 29) AND CURRENT_DATE
    GROUP BY product_id, search_word
) sw1 ON t.product_id = sw1.product_id AND sw1.rank = 1
WHERE
    t.traffic_date BETWEEN DATE_SUB(CURRENT_DATE, 29) AND CURRENT_DATE
GROUP BY
    t.product_id,
    t1.traffic_source,
    t1.visitors,
    sw1.search_word;


-- 创建表
drop table dws_force_metrics;
CREATE external TABLE dws_force_metrics(
    product_id              int COMMENT '商品ID',
    time_dimension          string COMMENT '时间维度，1天、7天、30天',
    current_inventory       int COMMENT '当前库存',
    inventory_salable_days int COMMENT '库存可售天数',
    price_force_star        int COMMENT '价格力星级',
    price_force_excellent_count int COMMENT '价格力优秀商品数量'
) COMMENT '库存与价格力相关汇总表'
LOCATION '/warehouse/online/dws/dws_force_metrics'
tblproperties (
'hive.exec.compress.output' = 'true',
'mapreduce.output.fileoutputformat.compress' = 'true',
'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);
select * from dws_force_metrics;
-- 插入数据
INSERT INTO TABLE dws_force_metrics
SELECT
    i.product_id,
    CASE
        WHEN MIN(i.inventory_date) = MAX(i.inventory_date) THEN '1天'
        WHEN DATEDIFF(MAX(i.inventory_date), MIN(i.inventory_date)) <= 6 THEN '7天'
        WHEN DATEDIFF(MAX(i.inventory_date), MIN(i.inventory_date)) <= 29 THEN '30天'
        ELSE NULL
    END AS time_dimension,
    i.current_inventory,
    -- 库存可售天数简单示例为当前库存除以当天销量（实际逻辑按需调整）
    CASE
        WHEN SUM(o.order_quantity) = 0 THEN 0
        ELSE i.current_inventory / SUM(o.order_quantity)
    END AS inventory_salable_days,
    pf.price_force_star,
    SUM(CASE WHEN pf.price_force_star >= 4 THEN 1 ELSE 0 END) AS price_force_excellent_count
FROM
    ods_inventory i
JOIN ods_orders o ON i.product_id = o.product_id AND i.inventory_date = o.order_date
JOIN ods_price_force pf ON i.product_id = pf.product_id AND i.inventory_date = pf.price_force_date
WHERE
    i.inventory_date BETWEEN DATE_SUB(CURRENT_DATE, 29) AND CURRENT_DATE
GROUP BY
    i.product_id,
    i.current_inventory,
    pf.price_force_star;








