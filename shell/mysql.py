import pymysql
from datetime import datetime, timedelta
import random
import time

# 数据库配置
DB_CONFIG = {
    'host': 'cdh03',
    'port': 3306,
    'user': 'root',
    'password': 'root',
    'database': 'electronic',
    'cursorclass': pymysql.cursors.DictCursor
}

# 模拟数据配置
PRODUCT_COUNT = 100  # 100个商品
TOTAL_RECORDS = 1000  # 总共模拟1000条记录
CATEGORIES = ['电子产品', '家居用品', '服装', '食品', '图书']
TRAFFIC_SOURCES = ['pc', 'mobile', 'app', 'search', 'recommend', 'direct', 'other']
PRICE_LEVELS = ['excellent', 'good', 'poor']


def create_database_connection():
    """创建数据库连接"""
    try:
        connection = pymysql.connect(**DB_CONFIG)
        print("成功连接到MySQL数据库")
        return connection
    except pymysql.Error as e:
        print(f"连接错误: {e}")
        return None


def create_tables(connection):
    """创建ODS表结构（所有ID字段为INT类型）"""
    with connection.cursor() as cursor:
        # 商品信息表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_info (
            product_id INT PRIMARY KEY AUTO_INCREMENT COMMENT '商品ID，自增主键',
            product_name VARCHAR(100) COMMENT '商品名称',
            category_id INT COMMENT '类目ID',
            category_name VARCHAR(50) COMMENT '类目名称',
            current_inventory INT COMMENT '当前库存数量',
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间'
        )COMMENT '商品基础信息表'
        """)

        # 销售明细表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_detail (
            id INT PRIMARY KEY AUTO_INCREMENT COMMENT '自增主键ID',
            order_id INT COMMENT '订单ID',
            product_id INT COMMENT '商品ID',
            sales_amount DECIMAL(18,2) COMMENT '销售金额',
            sales_volume INT COMMENT '销售数量',
            paying_buyer_id INT COMMENT '支付买家ID',
            paying_items INT COMMENT '支付商品件数',
            payment_time TIMESTAMP COMMENT '支付时间',
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间'
        )COMMENT '销售明细记录表'
        """)

        # 流量明细表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS traffic_detail (
            id INT PRIMARY KEY AUTO_INCREMENT COMMENT '自增主键ID',
            product_id INT COMMENT '商品ID',
            visitor_id INT COMMENT '访客ID',
            visit_time TIMESTAMP COMMENT '访问时间',
            traffic_source VARCHAR(20) COMMENT '流量来源',
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间'
        )COMMENT '商品流量明细表'
        """)

        # 搜索词表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS search_term (
            id INT PRIMARY KEY AUTO_INCREMENT COMMENT '自增主键ID',
            product_id INT COMMENT '商品ID',
            search_term VARCHAR(100) COMMENT '搜索关键词',
            search_count INT COMMENT '搜索次数',
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间'
        )COMMENT '商品搜索词表'
        """)

        # 价格力表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_power (
            id INT PRIMARY KEY AUTO_INCREMENT COMMENT '自增主键ID',
            product_id INT COMMENT '商品ID',
            price_power_star TINYINT COMMENT '价格力星级(1-5星)',
            inclusive_after_price DECIMAL(10,2) COMMENT '普惠券后价',
            price_power_level VARCHAR(20) COMMENT '价格力等级',
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间'
        )COMMENT '商品价格力评估表'
        """)

        connection.commit()
        print("表创建成功")


def generate_product_data(connection):
    """生成100个商品数据"""
    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE product_info")

        # 生成基础产品数据
        for i in range(1, PRODUCT_COUNT + 1):
            cursor.execute("""
            INSERT INTO product_info 
            (product_id, product_name, category_id, category_name, current_inventory)
            VALUES (%s, %s, %s, %s, %s)
            """, (
                i,  # 使用数字作为product_id
                f"商品{i}",
                random.randint(1, len(CATEGORIES)),  # category_id改为数字
                CATEGORIES[random.randint(0, len(CATEGORIES) - 1)],
                random.randint(10, 1000)
            ))

        connection.commit()
        print(f"生成 {PRODUCT_COUNT} 个商品数据成功")


def generate_sales_data(connection):
    """生成销售数据（400条）"""
    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE sales_detail")
        cursor.execute("SELECT product_id FROM product_info")
        product_ids = [row['product_id'] for row in cursor.fetchall()]

        for i in range(1, 401):
            cursor.execute("""
            INSERT INTO sales_detail 
            (order_id, product_id, sales_amount, sales_volume, paying_buyer_id, 
             paying_items, payment_time, create_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                i + 10000,  # 订单ID改为数字
                random.choice(product_ids),
                round(random.uniform(10, 500), 2) * random.randint(1, 5),
                random.randint(1, 5),
                random.randint(1000, 9999),  # 买家ID改为数字
                random.randint(1, 5),
                datetime.now() - timedelta(days=random.randint(0, 30)),
                datetime.now() - timedelta(days=random.randint(0, 30))
            ))

        connection.commit()
        print("生成400条销售记录成功")


def generate_traffic_data(connection):
    """生成流量数据（400条）"""
    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE traffic_detail")
        cursor.execute("SELECT product_id FROM product_info")
        product_ids = [row['product_id'] for row in cursor.fetchall()]

        for i in range(1, 401):
            visit_time = datetime.now() - timedelta(days=random.randint(0, 30))

            cursor.execute("""
            INSERT INTO traffic_detail 
            (product_id, visitor_id, visit_time, traffic_source, create_time)
            VALUES (%s, %s, %s, %s, %s)
            """, (
                random.choice(product_ids),
                random.randint(10000, 99999),  # 访客ID改为数字
                visit_time,
                random.choice(TRAFFIC_SOURCES),
                visit_time + timedelta(seconds=random.randint(1, 60))
            ))

        connection.commit()
        print("生成400条流量记录成功")


def generate_search_term_data(connection):
    """生成搜索词数据（100条）"""
    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE search_term")
        cursor.execute("SELECT product_id FROM product_info")
        product_ids = [row['product_id'] for row in cursor.fetchall()]

        search_terms = ["手机", "电脑", "沙发", "T恤", "零食", "小说", "耳机",
                        "手表", "鞋子", "牛奶", "玩具", "书包", "电视", "冰箱"]

        for i in range(1, 101):
            cursor.execute("""
            INSERT INTO search_term 
            (product_id, search_term, search_count, create_time)
            VALUES (%s, %s, %s, %s)
            """, (
                random.choice(product_ids),
                random.choice(search_terms),
                random.randint(1, 100),
                datetime.now() - timedelta(days=random.randint(0, 30))
            ))

        connection.commit()
        print("生成100条搜索词记录成功")


def generate_price_power_data(connection):
    """生成价格力数据（100条）"""
    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE price_power")
        cursor.execute("SELECT product_id FROM product_info")
        product_ids = [row['product_id'] for row in cursor.fetchall()]

        for i in range(1, 101):
            cursor.execute("""
            INSERT INTO price_power 
            (product_id, price_power_star, inclusive_after_price, price_power_level, create_time)
            VALUES (%s, %s, %s, %s, %s)
            """, (
                random.choice(product_ids),
                random.randint(1, 5),
                round(random.uniform(10, 500), 2),
                random.choice(PRICE_LEVELS),
                datetime.now() - timedelta(days=random.randint(0, 30))
            ))

        connection.commit()
        print("生成100条价格力记录成功")


def main():
    start_time = time.time()

    # 创建数据库连接
    connection = create_database_connection()
    if not connection:
        return

    try:
        # 创建表
        create_tables(connection)

        # 生成模拟数据
        generate_product_data(connection)  # 100条商品记录
        generate_sales_data(connection)  # 400条销售记录
        generate_traffic_data(connection)  # 400条流量记录
        generate_search_term_data(connection)  # 100条搜索记录
        generate_price_power_data(connection)  # 100条价格力记录

        # 验证总记录数
        with connection.cursor() as cursor:
            tables = ['product_info', 'sales_detail', 'traffic_detail',
                      'search_term', 'price_power']
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) as cnt FROM {table}")
                result = cursor.fetchone()
                print(f"{table} 记录数: {result['cnt']}")

        print(f"总共生成约1000条记录，耗时: {time.time() - start_time:.2f}秒")

    finally:
        if connection:
            connection.close()
            print("数据库连接已关闭")


if __name__ == "__main__":
    main()