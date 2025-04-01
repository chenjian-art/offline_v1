import pymysql
import random
from datetime import datetime, timedelta

# MySQL 连接配置
config = {
    'host': 'cdh03',
    'port': 3306,
    'user': 'root',
    'password': 'root',
    'database': 'dashboard',
    'cursorclass': pymysql.cursors.DictCursor
}


# 创建所有 ODS 表
def create_tables():
    conn = pymysql.connect(**config)
    cursor = conn.cursor()

    try:
        # 1. 用户行为日志表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_behavior (
            log_id int PRIMARY KEY COMMENT '日志ID',
            user_id int NOT NULL COMMENT '用户ID',
            item_id int NOT NULL COMMENT '商品ID',
            action VARCHAR(36) NOT NULL COMMENT '行为类型',
            channel VARCHAR(50) COMMENT '流量来源',
            search_term VARCHAR(255) COMMENT '搜索词',
            live_stream_id int COMMENT '直播ID',
            short_video_id int COMMENT '短视频ID',
            channel_id int NOT NULL COMMENT '渠道id',
            event_time DATETIME NOT NULL COMMENT '事件时间'
        ) COMMENT '用户行为原始表';
        """)

        # 2. 订单表：将表名 order 修改为 order_info
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS order_info (
            order_id int PRIMARY KEY COMMENT '订单ID',
            user_id int NOT NULL COMMENT '用户ID',
            order_time DATETIME NOT NULL COMMENT '下单时间',
            total_amount DECIMAL(10,2) NOT NULL COMMENT '订单总额',
            payment_method varchar(36) COMMENT '支付方式',
            promotion_id int COMMENT '促销活动ID'
        ) COMMENT '订单主表';
        """)

        # 3. 订单明细表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS order_detail (
            detail_id int PRIMARY KEY COMMENT '明细ID',
            order_id int NOT NULL COMMENT '订单ID',
            item_id int NOT NULL COMMENT '商品ID',
            sku_id int NOT NULL COMMENT 'SKUID',
            quantity INT NOT NULL COMMENT '购买数量',
            price DECIMAL(10,2) NOT NULL COMMENT '单价',
            attribute_combination varchar(255) COMMENT '属性组合'
        ) COMMENT '订单明细表';
        """)

        # 4. 商品表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS product (
            item_id int PRIMARY KEY COMMENT '商品ID',
            category_id int NOT NULL COMMENT '类目ID',
            base_price DECIMAL(10,2) NOT NULL COMMENT '基础价格',
            current_price DECIMAL(10,2) NOT NULL COMMENT '当前售价',
            stock INT NOT NULL COMMENT '库存数量',
            title VARCHAR(255) NOT NULL COMMENT '商品标题'
        ) COMMENT '商品基础信息表';
        """)

        # 5. 用户表：将表名 user 修改为 user_info
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_info (
            user_id int PRIMARY KEY COMMENT '用户ID',
            gender varchar(255) COMMENT '性别',
            age int UNSIGNED COMMENT '年龄',
            is_new_customer int NOT NULL DEFAULT 0 COMMENT '是否新客',
            register_date DATE NOT NULL COMMENT '注册日期'
        ) COMMENT '用户信息表';
        """)

        # 6. 促销活动表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS promotion (
            promotion_id int PRIMARY KEY COMMENT '活动ID',
            discount_type varchar(255) NOT NULL COMMENT '优惠类型',
            start_time DATETIME NOT NULL COMMENT '开始时间',
            end_time DATETIME NOT NULL COMMENT '结束时间'
        ) COMMENT '促销活动表';
        """)

        # 7. 评价表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS review (
            review_id int PRIMARY KEY COMMENT '评价ID',
            item_id int NOT NULL COMMENT '商品ID',
            user_id int NOT NULL COMMENT '用户ID',
            rating int NOT NULL COMMENT '评分(1 - 5)',
            review_time DATETIME NOT NULL COMMENT '评价时间',
            content TEXT COMMENT '评价内容'
        ) COMMENT '商品评价表';
        """)

        # 8. 渠道流量表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS channel_flow (
            channel_id int NOT NULL COMMENT '渠道id',
            channel_name VARCHAR(50) NOT NULL COMMENT '渠道名称',
            visit_count INT NOT NULL DEFAULT 0 COMMENT '访问量',
            conversion_count INT NOT NULL DEFAULT 0 COMMENT '转化量',
            PRIMARY KEY (channel_id)
        ) COMMENT '渠道流量统计表';
        """)

        # 9. 直播表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS live_stream (
            live_id int PRIMARY KEY COMMENT '直播ID',
            watch_duration int NOT NULL COMMENT '观看时长(秒)',
            interaction_count INT NOT NULL DEFAULT 0 COMMENT '互动次数',
            start_time DATETIME NOT NULL COMMENT '开始时间',
            end_time DATETIME NOT NULL COMMENT '结束时间'
        ) COMMENT '直播数据表';
        """)

        # 10. 短视频表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS short_video (
            video_id int PRIMARY KEY COMMENT '视频ID',
            like_count INT NOT NULL DEFAULT 0 COMMENT '点赞数',
            share_count INT NOT NULL DEFAULT 0 COMMENT '分享数',
            post_time DATETIME NOT NULL COMMENT '发布时间'
        ) COMMENT '短视频互动表';
        """)

        conn.commit()
        print("所有 ODS 表创建成功！")

    except pymysql.Error as e:
        print(f"创建表失败: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


# 模拟插入 500 条数据
def insert_mock_data():
    conn = pymysql.connect(**config)
    cursor = conn.cursor()

    try:
        # 生成用户数据
        user_ids = []
        for i in range(100):
            user_id = i + 1
            user_ids.append(user_id)
            gender = random.choice(['男', '女', '未知'])
            age = random.randint(18, 60)
            is_new_customer = random.randint(0, 1)
            register_date = datetime.now() - timedelta(days=random.randint(0, 730))
            cursor.execute("""
            INSERT INTO user_info (user_id, gender, age, is_new_customer, register_date)
            VALUES (%s, %s, %s, %s, %s)
            """, (user_id, gender, age, is_new_customer, register_date.date()))

        # 生成商品数据
        item_ids = []
        for i in range(100):
            item_id = i + 1
            item_ids.append(item_id)
            category_id = random.randint(1, 10)
            base_price = round(random.uniform(10, 1000), 2)
            current_price = round(random.uniform(10, base_price), 2)
            stock = random.randint(0, 1000)
            title = f'商品{i + 1}'
            cursor.execute("""
            INSERT INTO product (item_id, category_id, base_price, current_price, stock, title)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, (item_id, category_id, base_price, current_price, stock, title))

        # 生成促销活动数据
        promotion_ids = []
        for i in range(20):
            promotion_id = i + 1
            promotion_ids.append(promotion_id)
            discount_type = random.choice(['new_user', 'installment', 'full_reduction'])
            start_time = datetime.now() - timedelta(days=random.randint(0, 365))
            end_time = start_time + timedelta(days=random.randint(1, 30))
            cursor.execute("""
            INSERT INTO promotion (promotion_id, discount_type, start_time, end_time)
            VALUES (%s, %s, %s, %s)
            """, (promotion_id, discount_type, start_time, end_time))

        # 生成渠道流量数据
        channel_ids = []
        channels = ['搜索引擎', '社交媒体', '直接访问']
        for i, channel in enumerate(channels):
            channel_id = i + 1
            channel_ids.append(channel_id)
            visit_count = random.randint(100, 1000)
            conversion_count = random.randint(10, 100)
            cursor.execute("""
            INSERT INTO channel_flow (channel_id, channel_name, visit_count, conversion_count)
            VALUES (%s, %s, %s, %s)
            """, (channel_id, channel, visit_count, conversion_count))

        # 生成直播数据
        live_ids = []
        for i in range(20):
            live_id = i + 1
            live_ids.append(live_id)
            watch_duration = random.randint(60, 3600)
            interaction_count = random.randint(0, 100)
            start_time = datetime.now() - timedelta(days=random.randint(0, 365))
            end_time = start_time + timedelta(minutes=random.randint(30, 120))
            cursor.execute("""
            INSERT INTO live_stream (live_id, watch_duration, interaction_count, start_time, end_time)
            VALUES (%s, %s, %s, %s, %s)
            """, (live_id, watch_duration, interaction_count, start_time, end_time))

        # 生成短视频数据
        video_ids = []
        for i in range(20):
            video_id = i + 1
            video_ids.append(video_id)
            like_count = random.randint(0, 1000)
            share_count = random.randint(0, 100)
            post_time = datetime.now() - timedelta(days=random.randint(0, 365))
            cursor.execute("""
            INSERT INTO short_video (video_id, like_count, share_count, post_time)
            VALUES (%s, %s, %s, %s)
            """, (video_id, like_count, share_count, post_time))

        # 生成 500 条用户行为数据
        for i in range(500):
            log_id = i + 1
            user_id = random.choice(user_ids)
            item_id = random.choice(item_ids)
            action = random.choice(['click', 'cart', 'fav', 'pay'])
            channel = random.choice(channels)
            search_term = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=5))
            live_stream_id = random.choice(live_ids) if random.random() < 0.2 else None
            short_video_id = random.choice(video_ids) if random.random() < 0.2 else None
            channel_id = channel_ids[channels.index(channel)]
            event_time = datetime.now() - timedelta(days=random.randint(0, 365))
            cursor.execute("""
            INSERT INTO user_behavior (log_id, user_id, item_id, action, channel, search_term, live_stream_id, short_video_id, channel_id, event_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (log_id, user_id, item_id, action, channel, search_term, live_stream_id, short_video_id, channel_id, event_time))

            # 如果行为是支付，生成订单数据
            if action == 'pay':
                order_id = i + 1
                order_time = event_time
                total_amount = round(random.uniform(10, 1000), 2)
                payment_method = random.choice(['alipay', 'wechat', 'credit_card'])
                promotion_id = random.choice(promotion_ids) if random.random() < 0.5 else None
                cursor.execute("""
                INSERT INTO order_info (order_id, user_id, order_time, total_amount, payment_method, promotion_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                """, (order_id, user_id, order_time, total_amount, payment_method, promotion_id))

                # 生成订单明细数据
                detail_id = i + 1
                sku_id = random.randint(1, 1000)
                quantity = random.randint(1, 10)
                price = round(random.uniform(10, 100), 2)
                attribute_combination = f"颜色: {random.choice(['红', '蓝', '绿'])}, 尺寸: {random.choice(['S', 'M', 'L'])}"
                cursor.execute("""
                INSERT INTO order_detail (detail_id, order_id, item_id, sku_id, quantity, price, attribute_combination)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (detail_id, order_id, item_id, sku_id, quantity, price, attribute_combination))

        # 生成评价数据
        for i in range(100):
            review_id = i + 1
            item_id = random.choice(item_ids)
            user_id = random.choice(user_ids)
            rating = random.randint(1, 5)
            review_time = datetime.now() - timedelta(days=random.randint(0, 365))
            content = f'评价内容{i + 1}'
            cursor.execute("""
            INSERT INTO review (review_id, item_id, user_id, rating, review_time, content)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, (review_id, item_id, user_id, rating, review_time, content))

        conn.commit()
        print("500 条模拟数据插入成功！")

    except pymysql.Error as e:
        print(f"插入数据失败: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    create_tables()
    insert_mock_data()