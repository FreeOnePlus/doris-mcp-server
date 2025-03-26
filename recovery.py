#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
恢复脚本：修复元数据表结构并重新启动服务
"""

import os
import sys
import time
import subprocess
from dotenv import load_dotenv

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# 导入数据库工具
from src.doris_mcp.utils.db import execute_query

def fix_metadata_tables():
    """修复元数据表结构"""
    print("=" * 80)
    print("修复元数据表结构")
    print("=" * 80)
    
    # 加载环境变量
    load_dotenv()
    
    # 从环境变量获取连接信息
    db_name = os.getenv("DB_DATABASE", "ssb")
    metadata_db = f"{db_name}_metadata"
    
    print(f"使用数据库: {db_name}")
    print(f"元数据库: {metadata_db}")
    
    try:
        # 1. 检查并创建元数据数据库
        create_db_query = f"CREATE DATABASE IF NOT EXISTS `{metadata_db}`"
        print(f"\n执行: {create_db_query}")
        execute_query(create_db_query)
        print("✅ 元数据数据库创建/确认成功")
        
        # 2. 重新创建表结构元数据表
        drop_table_query = f"DROP TABLE IF EXISTS `{metadata_db}`.`table_metadata`"
        print(f"\n执行: {drop_table_query}")
        execute_query(drop_table_query)
        
        create_schema_table_query = f"""
        CREATE TABLE IF NOT EXISTS `{metadata_db}`.`table_metadata` (
            `table_name` VARCHAR(100) NOT NULL COMMENT '表名',
            `table_type` VARCHAR(50) COMMENT '表类型',
            `engine` VARCHAR(50) COMMENT '引擎类型',
            `table_comment` TEXT COMMENT '表注释',
            `column_info` TEXT COMMENT '列信息 (JSON格式)',
            `partition_info` TEXT COMMENT '分区信息 (JSON格式)',
            `business_summary` TEXT COMMENT '业务摘要',
            `update_time` DATETIME COMMENT '更新时间'
        )
        ENGINE=OLAP
        DUPLICATE KEY(`table_name`)
        COMMENT '表结构元数据'
        DISTRIBUTED BY HASH(`table_name`) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        print(f"\n执行: {create_schema_table_query}")
        execute_query(create_schema_table_query)
        print("✅ 表结构元数据表重建成功")
        
        # 3. 检查并创建SQL模式表
        drop_patterns_query = f"DROP TABLE IF EXISTS `{metadata_db}`.`sql_patterns`"
        print(f"\n执行: {drop_patterns_query}")
        execute_query(drop_patterns_query)
        
        create_sql_patterns_table_query = f"""
        CREATE TABLE IF NOT EXISTS `{metadata_db}`.`sql_patterns` (
            `pattern_id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '模式ID',
            `sql_type` VARCHAR(20) COMMENT 'SQL类型 (SELECT, INSERT等)',
            `simplified_sql` TEXT COMMENT '简化后的SQL',
            `examples` TEXT COMMENT 'SQL示例 (JSON格式)',
            `comments` TEXT COMMENT 'SQL注释 (JSON格式)',
            `frequency` INT COMMENT '出现频率',
            `tables` TEXT COMMENT '相关表 (JSON格式)',
            `update_time` DATETIME COMMENT '更新时间'
        )
        ENGINE=OLAP
        DUPLICATE KEY(`pattern_id`)
        COMMENT 'SQL查询模式'
        DISTRIBUTED BY HASH(`pattern_id`) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        print(f"\n执行: {create_sql_patterns_table_query}")
        execute_query(create_sql_patterns_table_query)
        print("✅ SQL模式表重建成功")
        
        # 4. 检查并创建业务元数据总结表
        drop_business_query = f"DROP TABLE IF EXISTS `{metadata_db}`.`business_metadata`"
        print(f"\n执行: {drop_business_query}")
        execute_query(drop_business_query)
        
        create_business_metadata_table_query = f"""
        CREATE TABLE IF NOT EXISTS `{metadata_db}`.`business_metadata` (
            `metadata_key` VARCHAR(50) NOT NULL COMMENT '元数据键',
            `metadata_value` TEXT COMMENT '元数据值 (JSON格式)',
            `update_time` DATETIME COMMENT '更新时间'
        )
        ENGINE=OLAP
        DUPLICATE KEY(`metadata_key`)
        COMMENT '业务元数据总结'
        DISTRIBUTED BY HASH(`metadata_key`) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        print(f"\n执行: {create_business_metadata_table_query}")
        execute_query(create_business_metadata_table_query)
        print("✅ 业务元数据总结表重建成功")
        
        print("\n所有元数据表已成功修复！")
        
    except Exception as e:
        print(f"\n❌ 执行过程中出错: {str(e)}")
        return False
        
    return True

def install_openai_package():
    """安装OpenAI包"""
    print("\n" + "=" * 80)
    print("安装OpenAI包")
    print("=" * 80)
    
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "openai"])
        print("✅ OpenAI包安装成功")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ 安装OpenAI包失败: {str(e)}")
        return False

def restart_service():
    """重启服务"""
    print("\n" + "=" * 80)
    print("重启MCP服务")
    print("=" * 80)
    
    # 终止所有python进程
    try:
        subprocess.run("pkill -f 'python src/main.py'", shell=True)
        print("✅ 已终止旧进程")
    except:
        print("⚠️ 没有找到旧进程")
    
    # 等待进程完全终止
    time.sleep(2)
    
    # 启动新进程
    print("\n启动服务...")
    os.environ["PYTHONPATH"] = os.path.abspath(os.path.dirname(__file__))
    
    try:
        # 使用nohup启动进程，将输出重定向到server_recovery.log
        cmd = f"nohup {sys.executable} src/main.py > server_recovery.log 2>&1 &"
        subprocess.run(cmd, shell=True)
        print(f"✅ 服务已在后台启动，日志文件: server_recovery.log")
        
        # 等待服务启动
        print("\n等待服务启动...")
        for i in range(5):
            time.sleep(1)
            print(".", end="", flush=True)
        print("\n")
        
        # 检查服务是否正在运行
        ps_output = subprocess.check_output("ps aux | grep 'python src/main.py' | grep -v grep", shell=True, text=True)
        if ps_output:
            print("✅ 服务进程已启动")
        else:
            print("❌ 服务进程未启动")
            
        # 检查端口是否被监听
        try:
            port_output = subprocess.check_output("lsof -i :3000", shell=True, text=True)
            print("✅ 服务正在监听端口3000")
            print(port_output)
        except subprocess.CalledProcessError:
            print("❌ 服务未监听端口3000，可能启动失败")
        
        return True
    except Exception as e:
        print(f"❌ 启动服务失败: {str(e)}")
        return False

def main():
    """主函数"""
    print("=" * 80)
    print("Doris MCP服务恢复工具")
    print("=" * 80)
    
    # 修复元数据表
    if fix_metadata_tables():
        print("\n✅ 元数据表修复成功")
    else:
        print("\n❌ 元数据表修复失败")
        return
    
    # 安装OpenAI包
    if install_openai_package():
        print("\n✅ 依赖包安装成功")
    else:
        print("\n❌ 依赖包安装失败")
    
    # 重启服务
    if restart_service():
        print("\n✅ 服务重启成功")
    else:
        print("\n❌ 服务重启失败")
    
    print("\n" + "=" * 80)
    print("恢复过程完成")
    print("=" * 80)
    print("\n请尝试访问服务: http://localhost:3000")
    print("如果服务仍然无法访问，请检查 server_recovery.log 日志文件")

if __name__ == "__main__":
    main() 