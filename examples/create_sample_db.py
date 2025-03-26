#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
创建示例数据库

这个脚本用于在Apache Doris中创建示例表和数据，
以便完整测试NL2SQL功能。
"""

import os
import sys
from dotenv import load_dotenv

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 导入数据库工具
from src.doris_mcp.utils.db import execute_query, get_db_connection

# 示例表创建和数据插入SQL
CREATE_TABLES_SQL = [
    # 产品表
    """
    CREATE TABLE IF NOT EXISTS products (
        id INT NOT NULL COMMENT '产品ID',
        product_name VARCHAR(100) NOT NULL COMMENT '产品名称',
        category VARCHAR(50) COMMENT '产品类别',
        price DECIMAL(10,2) COMMENT '价格',
        cost DECIMAL(10,2) COMMENT '成本',
        inventory INT COMMENT '库存数量'
    )
    ENGINE=OLAP
    DUPLICATE KEY(id)
    COMMENT '产品表，包含产品信息'
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num" = "1");
    """,
    
    # 订单表
    """
    CREATE TABLE IF NOT EXISTS orders (
        order_id INT NOT NULL COMMENT '订单ID',
        customer_id INT COMMENT '客户ID',
        order_date DATE COMMENT '订单日期',
        store_id INT COMMENT '商店ID',
        total_amount DECIMAL(10,2) COMMENT '订单总金额',
        create_time DATETIME COMMENT '创建时间'
    )
    ENGINE=OLAP
    DUPLICATE KEY(order_id)
    COMMENT '订单表，包含订单基本信息'
    DISTRIBUTED BY HASH(order_id) BUCKETS 1
    PROPERTIES("replication_num" = "1");
    """,
    
    # 订单明细表
    """
    CREATE TABLE IF NOT EXISTS order_items (
        order_id INT NOT NULL COMMENT '订单ID',
        product_id INT NOT NULL COMMENT '产品ID',
        quantity INT COMMENT '数量',
        price DECIMAL(10,2) COMMENT '单价'
    )
    ENGINE=OLAP
    DUPLICATE KEY(order_id, product_id)
    COMMENT '订单明细表，包含订单中的商品信息'
    DISTRIBUTED BY HASH(order_id) BUCKETS 1
    PROPERTIES("replication_num" = "1");
    """,
    
    # 客户表
    """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INT NOT NULL COMMENT '客户ID',
        customer_name VARCHAR(100) COMMENT '客户名称',
        email VARCHAR(100) COMMENT '电子邮件',
        phone VARCHAR(20) COMMENT '电话号码',
        register_date DATE COMMENT '注册日期'
    )
    ENGINE=OLAP
    DUPLICATE KEY(customer_id)
    COMMENT '客户表，包含客户基本信息'
    DISTRIBUTED BY HASH(customer_id) BUCKETS 1
    PROPERTIES("replication_num" = "1");
    """,
    
    # 区域表
    """
    CREATE TABLE IF NOT EXISTS regions (
        region_id INT NOT NULL COMMENT '区域ID',
        region_name VARCHAR(50) COMMENT '区域名称',
        country VARCHAR(50) COMMENT '国家'
    )
    ENGINE=OLAP
    DUPLICATE KEY(region_id)
    COMMENT '区域表，包含销售区域信息'
    DISTRIBUTED BY HASH(region_id) BUCKETS 1
    PROPERTIES("replication_num" = "1");
    """,
    
    # 商店表
    """
    CREATE TABLE IF NOT EXISTS stores (
        store_id INT NOT NULL COMMENT '商店ID',
        store_name VARCHAR(100) COMMENT '商店名称',
        region_id INT COMMENT '区域ID',
        address VARCHAR(200) COMMENT '地址'
    )
    ENGINE=OLAP
    DUPLICATE KEY(store_id)
    COMMENT '商店表，包含商店信息'
    DISTRIBUTED BY HASH(store_id) BUCKETS 1
    PROPERTIES("replication_num" = "1");
    """,
    
    # 用户活动表
    """
    CREATE TABLE IF NOT EXISTS user_activities (
        id INT NOT NULL COMMENT '活动ID',
        user_id INT COMMENT '用户ID',
        activity_type VARCHAR(50) COMMENT '活动类型',
        activity_time DATETIME COMMENT '活动时间',
        details VARCHAR(200) COMMENT '活动详情'
    )
    ENGINE=OLAP
    DUPLICATE KEY(id)
    COMMENT '用户活动表，记录用户行为'
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num" = "1");
    """
]

# 示例数据
INSERT_DATA_SQL = [
    # 产品表数据
    """
    INSERT INTO products VALUES
    (1, '智能手机A', '电子产品', 2999.00, 1800.00, 100),
    (2, '笔记本电脑B', '电子产品', 5999.00, 4200.00, 50),
    (3, '蓝牙耳机C', '配件', 299.00, 150.00, 200),
    (4, '平板电脑D', '电子产品', 1999.00, 1200.00, 80),
    (5, '智能手表E', '配件', 899.00, 500.00, 120),
    (6, '相机F', '电子产品', 3999.00, 2800.00, 30),
    (7, '游戏机G', '电子产品', 2499.00, 1700.00, 40),
    (8, '充电宝H', '配件', 199.00, 80.00, 300),
    (9, '键盘I', '配件', 399.00, 180.00, 150),
    (10, '显示器J', '电子产品', 1299.00, 800.00, 60);
    """,
    
    # 区域表数据
    """
    INSERT INTO regions VALUES
    (1, '华东', '中国'),
    (2, '华南', '中国'),
    (3, '华北', '中国'),
    (4, '华中', '中国'),
    (5, '西南', '中国');
    """,
    
    # 商店表数据
    """
    INSERT INTO stores VALUES
    (1, '旗舰店一', 1, '上海市黄浦区南京东路123号'),
    (2, '旗舰店二', 2, '广州市天河区天河路456号'),
    (3, '旗舰店三', 3, '北京市朝阳区朝阳路789号'),
    (4, '专卖店一', 1, '南京市鼓楼区中山路111号'),
    (5, '专卖店二', 4, '武汉市洪山区珞瑜路222号');
    """,
    
    # 客户表数据
    """
    INSERT INTO customers VALUES
    (1, '张三', 'zhangsan@example.com', '13800001111', '2022-01-01'),
    (2, '李四', 'lisi@example.com', '13900002222', '2022-02-15'),
    (3, '王五', 'wangwu@example.com', '13700003333', '2022-03-10'),
    (4, '赵六', 'zhaoliu@example.com', '13600004444', '2022-04-20'),
    (5, '孙七', 'sunqi@example.com', '13500005555', '2022-05-05');
    """,
    
    # 订单表数据
    """
    INSERT INTO orders VALUES
    (1001, 1, '2023-01-15', 1, 3298.00, '2023-01-15 10:30:00'),
    (1002, 2, '2023-01-20', 2, 6298.00, '2023-01-20 14:20:00'),
    (1003, 3, '2023-02-05', 3, 4998.00, '2023-02-05 16:45:00'),
    (1004, 1, '2023-02-15', 1, 599.00, '2023-02-15 09:15:00'),
    (1005, 4, '2023-03-01', 4, 1299.00, '2023-03-01 11:30:00'),
    (1006, 5, '2023-03-10', 5, 3999.00, '2023-03-10 15:20:00'),
    (1007, 2, '2023-03-20', 2, 199.00, '2023-03-20 17:10:00'),
    (1008, 3, '2023-04-05', 3, 1998.00, '2023-04-05 10:45:00'),
    (1009, 4, '2023-04-15', 4, 6398.00, '2023-04-15 13:30:00'),
    (1010, 1, '2023-05-01', 1, 2999.00, '2023-05-01 09:50:00'),
    (1011, 1, '2022-05-15', 1, 3298.00, '2022-05-15 10:30:00'),
    (1012, 2, '2022-06-20', 2, 6298.00, '2022-06-20 14:20:00');
    """,
    
    # 订单明细表数据
    """
    INSERT INTO order_items VALUES
    (1001, 1, 1, 2999.00),
    (1001, 3, 1, 299.00),
    (1002, 2, 1, 5999.00),
    (1002, 3, 1, 299.00),
    (1003, 1, 1, 2999.00),
    (1003, 5, 1, 899.00),
    (1003, 8, 5, 199.00),
    (1004, 3, 1, 299.00),
    (1004, 8, 1, 199.00),
    (1004, 9, 1, 399.00),
    (1005, 10, 1, 1299.00),
    (1006, 6, 1, 3999.00),
    (1007, 8, 1, 199.00),
    (1008, 4, 1, 1999.00),
    (1009, 2, 1, 5999.00),
    (1009, 3, 1, 299.00),
    (1009, 9, 1, 399.00),
    (1010, 1, 1, 2999.00),
    (1011, 1, 1, 2999.00),
    (1011, 3, 1, 299.00),
    (1012, 2, 1, 5999.00),
    (1012, 3, 1, 299.00);
    """,
    
    # 用户活动表数据
    """
    INSERT INTO user_activities VALUES
    (1, 1, '登录', '2023-04-01 08:30:00', '用户登录'),
    (2, 1, '浏览', '2023-04-01 08:35:00', '浏览商品ID=1'),
    (3, 1, '加入购物车', '2023-04-01 08:40:00', '加入商品ID=1'),
    (4, 1, '购买', '2023-04-01 08:45:00', '购买商品ID=1'),
    (5, 2, '登录', '2023-04-01 09:15:00', '用户登录'),
    (6, 2, '浏览', '2023-04-01 09:20:00', '浏览商品ID=2'),
    (7, 2, '加入购物车', '2023-04-01 09:25:00', '加入商品ID=2'),
    (8, 2, '购买', '2023-04-01 09:30:00', '购买商品ID=2'),
    (9, 3, '登录', '2023-04-02 10:10:00', '用户登录'),
    (10, 3, '浏览', '2023-04-02 10:15:00', '浏览商品ID=3'),
    (11, 4, '登录', '2023-04-03 14:20:00', '用户登录'),
    (12, 4, '浏览', '2023-04-03 14:25:00', '浏览商品ID=4'),
    (13, 5, '登录', '2023-04-04 16:30:00', '用户登录'),
    (14, 5, '浏览', '2023-04-04 16:35:00', '浏览商品ID=5'),
    (15, 5, '加入购物车', '2023-04-04 16:40:00', '加入商品ID=5');
    """
]

def create_sample_database():
    """创建示例数据库和表"""
    print("=" * 80)
    print("Apache Doris NL2SQL 示例数据库创建工具")
    print("=" * 80)
    
    # 加载环境变量
    load_dotenv()
    
    # 连接信息
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "9030")
    db_user = os.getenv("DB_USER", "root")
    db_pass = os.getenv("DB_PASSWORD", "")
    db_name = os.getenv("DB_DATABASE", "")
    
    print(f"数据库连接信息: {db_host}:{db_port}, 用户: {db_user}, 数据库: {db_name}\n")
    
    # 确认是否继续
    confirm = input("此操作将在您的数据库中创建示例表。是否继续? (y/n): ")
    if confirm.lower() != 'y':
        print("操作已取消")
        return
    
    # 创建表
    print("\n创建示例表...")
    for i, create_sql in enumerate(CREATE_TABLES_SQL, 1):
        try:
            execute_query(create_sql)
            print(f"表 {i}/7 创建成功")
        except Exception as e:
            print(f"创建表 {i} 时出错: {str(e)}")
    
    # 插入数据
    print("\n插入示例数据...")
    for i, insert_sql in enumerate(INSERT_DATA_SQL, 1):
        try:
            execute_query(insert_sql)
            print(f"数据集 {i}/{len(INSERT_DATA_SQL)} 插入成功")
        except Exception as e:
            print(f"插入数据集 {i} 时出错: {str(e)}")
    
    print("\n示例数据库创建完成！现在您可以使用示例NL2SQL查询进行测试。")
    print("示例查询：")
    print("1. 查询销售额前10的产品及其销售数量")
    print("2. 计算每个区域今年与去年同期的销售额对比")
    print("3. 分析过去30天内每个用户的活跃度")
    print("4. 统计各区域的销售总额和订单数量")

if __name__ == "__main__":
    create_sample_database() 