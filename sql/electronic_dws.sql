use electronic;


CREATE TABLE dws_product_performance (
    stat_period STRING COMMENT '统计周期：1d/7d/30d',
    product_id int COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_id int COMMENT '类目ID',
    category_name STRING COMMENT '类目名称',
    sales_amount DECIMAL(10,2) COMMENT '销售额',
    sales_volume int COMMENT '销量',
    paying_buyers int COMMENT '支付买家数',
    payment_conversion_rate DECIMAL(10,4) COMMENT '支付转化率(支付买家数/商品访客数)',
    paying_items int COMMENT '支付件数',
    traffic_stats STRING COMMENT '流量统计信息',
    category_sales_share DECIMAL(10,4) COMMENT '类目销售占比',
    market_sales_share DECIMAL(10,4) COMMENT '市场销售占比',
    current_inventory int COMMENT '当前库存',
    inventory_sale_days int COMMENT '库存可售天数',
    top10_search_terms string COMMENT 'Top10搜索词',
    price_power_stats STRING COMMENT '价格力统计信息',
    product_power_warning STRING COMMENT '商品力预警',
    price_power_warning STRING COMMENT '价格力预警',
    dw_create_time string COMMENT '数据创建时间',
    dw_update_time string COMMENT '数据更新时间'
)COMMENT"汇总表"
location "/warehouse/electronic/dws/dws_product_performance"
tblproperties (
'hive.exec.compress.output' = 'true',
'mapreduce.output.fileoutputformat.compress' = 'true',
'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);
select * from dws_product_performance;




-- 第三版
INSERT OVERWRITE TABLE dws_product_performance
SELECT
    stat_period,
    product_id,
    product_name,
    category_id,
    category_name,
    sales_amount,
    sales_volume,
    paying_buyers,
    payment_conversion_rate,
    paying_items,
    traffic_stats,
    category_sales_share *100,
    market_sales_share * 100,
    current_inventory,
    inventory_sale_days,
    top10_search_terms,
    price_power_stats,
    product_power_warning,
    price_power_warning,
    dw_create_time,
    dw_update_time
FROM (
    SELECT
        -- 根据create_time判断统计周期
        CASE
            WHEN MIN(sd.create_time) = MAX(sd.create_time) THEN '1d'
            WHEN DATEDIFF(MAX(sd.create_time), MIN(sd.create_time)) <= 6 THEN '7d'
            WHEN DATEDIFF(MAX(sd.create_time), MIN(sd.create_time)) <= 29 THEN '30d'
            ELSE NULL
        END AS stat_period,
        pi.product_id,
        pi.product_name,
        pi.category_id,
        pi.category_name,
        SUM(COALESCE(sd.sales_amount, 0)) AS sales_amount,
        SUM(COALESCE(sd.sales_volume, 0)) AS sales_volume,
        COUNT(DISTINCT sd.paying_buyer_id) AS paying_buyers,
        --计算支付转化率
        CASE
            WHEN SUM(COALESCE(td.visitor_id, 0)) = 0 THEN 0
            ELSE ROUND(COUNT(DISTINCT sd.paying_buyer_id) / SUM(COALESCE(td.visitor_id, 0)), 4)
        END AS payment_conversion_rate,
        SUM(COALESCE(sd.paying_items, 0)) AS paying_items,
        --构建流量统计信息
        CONCAT(
            'total_visitors:', COALESCE(COUNT(DISTINCT td.visitor_id), 0),
            ',pc_visitors:', COALESCE(SUM(CASE WHEN td.traffic_source = 'pc' THEN 1 ELSE 0 END), 0),
            ',mobile_visitors:', COALESCE(SUM(CASE WHEN td.traffic_source ='mobile' THEN 1 ELSE 0 END), 0),
            ',app_visitors:', COALESCE(SUM(CASE WHEN td.traffic_source = 'app' THEN 1 ELSE 0 END), 0),
            ',search_visitors:', COALESCE(SUM(CASE WHEN td.traffic_source ='search' THEN 1 ELSE 0 END), 0)
        ) AS traffic_stats,
        --计算类目销售占比
        ROUND(
            CASE
                WHEN SUM(SUM(COALESCE(sd.sales_amount, 0))) OVER (PARTITION BY pi.category_id) = 0 THEN 0
                ELSE SUM(COALESCE(sd.sales_amount, 0)) / SUM(SUM(COALESCE(sd.sales_amount, 0))) OVER (PARTITION BY pi.category_id)
            END,
            4
        ) AS category_sales_share,
        --计算市场销售占比
        ROUND(
            CASE
                WHEN SUM(SUM(COALESCE(sd.sales_amount, 0))) OVER () = 0 THEN 0
                ELSE SUM(COALESCE(sd.sales_amount, 0)) / SUM(SUM(COALESCE(sd.sales_amount, 0))) OVER ()
            END,
            4
        ) AS market_sales_share,
        AVG(COALESCE(pi.current_inventory, 0)) AS current_inventory,
        --计算库存可售天数
        CASE
            WHEN SUM(COALESCE(sd.sales_volume, 0)) = 0 THEN 9999
            ELSE AVG(COALESCE(pi.current_inventory, 0)) / SUM(COALESCE(sd.sales_volume, 0))
        END AS inventory_sale_days,
        --获取Top10搜索词
        COLLECT_LIST(st.search_term) AS top10_search_terms,
        --构建价格力统计信息
        CONCAT(
            'star:', COALESCE(MAX(pp.price_power_star), 0),
            ',excellent_count:', COALESCE(SUM(CASE WHEN pp.price_power_level = 'excellent' THEN 1 ELSE 0 END), 0),
            ',good_count:', COALESCE(SUM(CASE WHEN pp.price_power_level = 'good' THEN 1 ELSE 0 END), 0),
            ',poor_count:', COALESCE(SUM(CASE WHEN pp.price_power_level = 'poor' THEN 1 ELSE 0 END), 0),
            ',inclusive_price:', COALESCE(MAX(pp.inclusive_after_price), 0)
        ) AS price_power_stats,
        --商品力预警
        CASE
            WHEN SUM(COALESCE(sd.sales_volume, 0)) < 10 THEN '低销量预警'
            WHEN AVG(COALESCE(pi.current_inventory, 0)) < 50 THEN '低库存预警'
            ELSE '正常'
        END AS product_power_warning,
        --价格力预警
        CASE
            WHEN COALESCE(MAX(pp.price_power_star), 0) < 3 THEN '价格力不足预警'
            ELSE '正常'
        END AS price_power_warning,
        CURRENT_TIMESTAMP AS dw_create_time,
        CURRENT_TIMESTAMP AS dw_update_time
    FROM
        ods_product_info pi
    --关联销售明细表获取销售相关数据
    LEFT JOIN ods_sales_detail sd ON pi.product_id = sd.product_id
    --关联流量明细表获取流量相关数据
    LEFT JOIN ods_traffic_detail td ON pi.product_id = td.product_id
    --关联商品搜索词表获取搜索词相关数据
    LEFT JOIN ods_search_term st ON pi.product_id = st.product_id
    --关联商品价格力评估表获取价格力相关数据
    LEFT JOIN ods_price_power pp ON pi.product_id = pp.product_id
    GROUP BY
        pi.product_id,
        pi.product_name,
        pi.category_id,
        pi.category_name
) AS subquery
WHERE stat_period IS NOT NULL;





SELECT
    stat_period,
    product_id,
    product_name,
    category_id,
    category_name,
    sales_amount,
    sales_volume,
    paying_buyers,
    payment_conversion_rate,
    paying_items,
    total_visitors,
    pc_visitors,
    mobile_visitors,
    app_visitors,
    search_visitors,
    category_sales_share * 100 AS category_sales_share_percent,
    market_sales_share * 100 AS market_sales_share_percent,
    current_inventory,
    inventory_sale_days,
    top10_search_terms,
    price_power_star,
    excellent_count,
    good_count,
    poor_count,
    inclusive_price,
    product_power_warning,
    price_power_warning,
    dw_create_time,
    dw_update_time
FROM (
    SELECT
        -- 根据create_time判断统计周期
        CASE
            WHEN MIN(sd.create_time) = MAX(sd.create_time) THEN '1d'
            WHEN DATEDIFF(MAX(sd.create_time), MIN(sd.create_time)) <= 6 THEN '7d'
            WHEN DATEDIFF(MAX(sd.create_time), MIN(sd.create_time)) <= 29 THEN '30d'
            ELSE NULL
        END AS stat_period,
        pi.product_id,
        pi.product_name,
        pi.category_id,
        pi.category_name,
        SUM(COALESCE(sd.sales_amount, 0)) AS sales_amount,
        SUM(COALESCE(sd.sales_volume, 0)) AS sales_volume,
        COUNT(DISTINCT sd.paying_buyer_id) AS paying_buyers,
        -- 计算支付转化率
        CASE
            WHEN SUM(COALESCE(td.visitor_id, 0)) = 0 THEN 0
            ELSE ROUND(COUNT(DISTINCT sd.paying_buyer_id) / SUM(COALESCE(td.visitor_id, 0)), 4)
        END AS payment_conversion_rate,
        SUM(COALESCE(sd.paying_items, 0)) AS paying_items,
        -- 流量统计信息
        COUNT(DISTINCT td.visitor_id) AS total_visitors,
        SUM(CASE WHEN td.traffic_source = 'pc' THEN 1 ELSE 0 END) AS pc_visitors,
        SUM(CASE WHEN td.traffic_source ='mobile' THEN 1 ELSE 0 END) AS mobile_visitors,
        SUM(CASE WHEN td.traffic_source = 'app' THEN 1 ELSE 0 END) AS app_visitors,
        SUM(CASE WHEN td.traffic_source ='search' THEN 1 ELSE 0 END) AS search_visitors,
        -- 计算类目销售占比
        ROUND(
            CASE
                WHEN SUM(SUM(COALESCE(sd.sales_amount, 0))) OVER (PARTITION BY pi.category_id) = 0 THEN 0
                ELSE SUM(COALESCE(sd.sales_amount, 0)) / SUM(SUM(COALESCE(sd.sales_amount, 0))) OVER (PARTITION BY pi.category_id)
            END,
            4
        ) AS category_sales_share,
        -- 计算市场销售占比
        ROUND(
            CASE
                WHEN SUM(SUM(COALESCE(sd.sales_amount, 0))) OVER () = 0 THEN 0
                ELSE SUM(COALESCE(sd.sales_amount, 0)) / SUM(SUM(COALESCE(sd.sales_amount, 0))) OVER ()
            END,
            4
        ) AS market_sales_share,
        AVG(COALESCE(pi.current_inventory, 0)) AS current_inventory,
        -- 计算库存可售天数
        CASE
            WHEN SUM(COALESCE(sd.sales_volume, 0)) = 0 THEN 9999
            ELSE AVG(COALESCE(pi.current_inventory, 0)) / SUM(COALESCE(sd.sales_volume, 0))
        END AS inventory_sale_days,
        -- 获取Top10搜索词
        COLLECT_LIST(st.search_term) AS top10_search_terms,
        -- 价格力统计信息
        MAX(pp.price_power_star) AS price_power_star,
        SUM(CASE WHEN pp.price_power_level = 'excellent' THEN 1 ELSE 0 END) AS excellent_count,
        SUM(CASE WHEN pp.price_power_level = 'good' THEN 1 ELSE 0 END) AS good_count,
        SUM(CASE WHEN pp.price_power_level = 'poor' THEN 1 ELSE 0 END) AS poor_count,
        MAX(pp.inclusive_after_price) AS inclusive_price,
        -- 商品力预警
        CASE
            WHEN SUM(COALESCE(sd.sales_volume, 0)) < 10 THEN '低销量预警'
            WHEN AVG(COALESCE(pi.current_inventory, 0)) < 50 THEN '低库存预警'
            ELSE '正常'
        END AS product_power_warning,
        -- 价格力预警
        CASE
            WHEN COALESCE(MAX(pp.price_power_star), 0) < 3 THEN '价格力不足预警'
            ELSE '正常'
        END AS price_power_warning,
        CURRENT_TIMESTAMP AS dw_create_time,
        CURRENT_TIMESTAMP AS dw_update_time
    FROM
        ods_product_info pi
    -- 关联销售明细表获取销售相关数据
    LEFT JOIN ods_sales_detail sd ON pi.product_id = sd.product_id
    -- 关联流量明细表获取流量相关数据
    LEFT JOIN ods_traffic_detail td ON pi.product_id = td.product_id
    -- 关联商品搜索词表获取搜索词相关数据
    LEFT JOIN ods_search_term st ON pi.product_id = st.product_id
    -- 关联商品价格力评估表获取价格力相关数据
    LEFT JOIN ods_price_power pp ON pi.product_id = pp.product_id
    GROUP BY
        pi.product_id,
        pi.product_name,
        pi.category_id,
        pi.category_name
) AS subquery
WHERE stat_period IS NOT NULL;