#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
系统资源处理函数
"""

import os
import sys
import logging
import psutil
import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_system_status() -> str:
    """
    获取系统状态信息
    
    Returns:
        str: 系统状态信息（HTML格式）
    """
    try:
        # 获取系统信息
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        # 构建HTML内容
        html = "<h1>系统状态</h1>"
        
        # 基本信息
        html += "<h2>基本信息</h2>"
        html += "<table border='1'>"
        html += "<tr><th>指标</th><th>值</th></tr>"
        html += f"<tr><td>主机名</td><td>{os.uname().nodename}</td></tr>"
        html += f"<tr><td>操作系统</td><td>{os.uname().sysname} {os.uname().release}</td></tr>"
        html += f"<tr><td>Python版本</td><td>{sys.version.split()[0]}</td></tr>"
        html += f"<tr><td>当前时间</td><td>{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</td></tr>"
        html += f"<tr><td>运行时间</td><td>{datetime.timedelta(seconds=int(psutil.boot_time()))}</td></tr>"
        html += "</table>"
        
        # 资源使用情况
        html += "<h2>资源使用情况</h2>"
        html += "<table border='1'>"
        html += "<tr><th>资源</th><th>使用率</th><th>已用/总量</th></tr>"
        html += f"<tr><td>CPU</td><td>{cpu_percent}%</td><td>-</td></tr>"
        html += f"<tr><td>内存</td><td>{memory.percent}%</td><td>{memory.used/(1024*1024*1024):.2f}GB / {memory.total/(1024*1024*1024):.2f}GB</td></tr>"
        html += f"<tr><td>磁盘</td><td>{disk.percent}%</td><td>{disk.used/(1024*1024*1024):.2f}GB / {disk.total/(1024*1024*1024):.2f}GB</td></tr>"
        html += "</table>"
        
        # 进程信息
        html += "<h2>进程信息</h2>"
        current_process = psutil.Process()
        html += "<table border='1'>"
        html += "<tr><th>指标</th><th>值</th></tr>"
        html += f"<tr><td>进程ID</td><td>{current_process.pid}</td></tr>"
        html += f"<tr><td>用户</td><td>{current_process.username()}</td></tr>"
        html += f"<tr><td>启动时间</td><td>{datetime.datetime.fromtimestamp(current_process.create_time()).strftime('%Y-%m-%d %H:%M:%S')}</td></tr>"
        html += f"<tr><td>CPU使用率</td><td>{current_process.cpu_percent(interval=1)}%</td></tr>"
        html += f"<tr><td>内存使用</td><td>{current_process.memory_info().rss/(1024*1024):.2f}MB</td></tr>"
        html += "</table>"
        
        logger.info("成功获取系统状态信息")
        return html
    except Exception as e:
        logger.error(f"获取系统状态时出错: {str(e)}")
        return f"<h1>Error</h1><p>获取系统状态时出错: {str(e)}</p>"

def get_system_performance() -> str:
    """
    获取系统性能指标
    
    Returns:
        str: 系统性能指标（HTML格式）
    """
    try:
        # 构建HTML内容
        html = "<h1>系统性能</h1>"
        
        # CPU信息
        html += "<h2>CPU性能</h2>"
        html += "<table border='1'>"
        html += "<tr><th>指标</th><th>值</th></tr>"
        html += f"<tr><td>CPU核心数</td><td>{psutil.cpu_count(logical=False)} 物理核心 ({psutil.cpu_count()} 逻辑核心)</td></tr>"
        html += f"<tr><td>CPU使用率</td><td>{psutil.cpu_percent(interval=1)}%</td></tr>"
        
        # CPU每个核心的使用率
        cpu_percent_per_core = psutil.cpu_percent(interval=1, percpu=True)
        for i, percent in enumerate(cpu_percent_per_core):
            html += f"<tr><td>核心 {i} 使用率</td><td>{percent}%</td></tr>"
        
        html += "</table>"
        
        # 内存信息
        memory = psutil.virtual_memory()
        html += "<h2>内存性能</h2>"
        html += "<table border='1'>"
        html += "<tr><th>指标</th><th>值</th></tr>"
        html += f"<tr><td>总内存</td><td>{memory.total/(1024*1024*1024):.2f} GB</td></tr>"
        html += f"<tr><td>可用内存</td><td>{memory.available/(1024*1024*1024):.2f} GB</td></tr>"
        html += f"<tr><td>已用内存</td><td>{memory.used/(1024*1024*1024):.2f} GB</td></tr>"
        html += f"<tr><td>内存使用率</td><td>{memory.percent}%</td></tr>"
        html += "</table>"
        
        # 磁盘信息
        html += "<h2>磁盘性能</h2>"
        html += "<table border='1'>"
        html += "<tr><th>挂载点</th><th>总空间</th><th>已用空间</th><th>可用空间</th><th>使用率</th></tr>"
        
        for partition in psutil.disk_partitions():
            try:
                usage = psutil.disk_usage(partition.mountpoint)
                html += "<tr>"
                html += f"<td>{partition.mountpoint}</td>"
                html += f"<td>{usage.total/(1024*1024*1024):.2f} GB</td>"
                html += f"<td>{usage.used/(1024*1024*1024):.2f} GB</td>"
                html += f"<td>{usage.free/(1024*1024*1024):.2f} GB</td>"
                html += f"<td>{usage.percent}%</td>"
                html += "</tr>"
            except:
                # 跳过无法访问的磁盘
                pass
        
        html += "</table>"
        
        # 网络信息
        html += "<h2>网络性能</h2>"
        html += "<table border='1'>"
        html += "<tr><th>接口</th><th>字节发送</th><th>字节接收</th></tr>"
        
        net_io_counters = psutil.net_io_counters(pernic=True)
        for interface, io_counter in net_io_counters.items():
            html += "<tr>"
            html += f"<td>{interface}</td>"
            html += f"<td>{io_counter.bytes_sent/(1024*1024):.2f} MB</td>"
            html += f"<td>{io_counter.bytes_recv/(1024*1024):.2f} MB</td>"
            html += "</tr>"
        
        html += "</table>"
        
        logger.info("成功获取系统性能信息")
        return html
    except Exception as e:
        logger.error(f"获取系统性能时出错: {str(e)}")
        return f"<h1>Error</h1><p>获取系统性能时出错: {str(e)}</p>"

def get_system_logs() -> str:
    """
    获取系统日志
    
    Returns:
        str: 系统日志（HTML格式）
    """
    try:
        log_file = os.path.join(os.path.dirname(__file__), '../../logs/app.log')
        
        if not os.path.exists(log_file):
            return "<h1>系统日志</h1><p>日志文件不存在</p>"
        
        # 读取最近的日志（最多1000行）
        with open(log_file, 'r', encoding='utf-8') as f:
            logs = f.readlines()[-1000:]
        
        # 构建HTML内容
        html = "<h1>系统日志</h1>"
        html += f"<p>显示最近 {len(logs)} 条日志记录</p>"
        
        html += "<pre style='background-color: #f5f5f5; padding: 10px; border-radius: 5px; overflow: auto; max-height: 500px;'>"
        for log in logs:
            # 根据日志级别设置不同的颜色
            if "ERROR" in log:
                html += f"<span style='color: red;'>{log}</span>"
            elif "WARNING" in log:
                html += f"<span style='color: orange;'>{log}</span>"
            elif "INFO" in log:
                html += f"<span style='color: green;'>{log}</span>"
            else:
                html += log
        html += "</pre>"
        
        logger.info("成功获取系统日志")
        return html
    except Exception as e:
        logger.error(f"获取系统日志时出错: {str(e)}")
        return f"<h1>Error</h1><p>获取系统日志时出错: {str(e)}</p>"

def get_system_audit() -> str:
    """
    获取审计日志
    
    Returns:
        str: 审计日志（HTML格式）
    """
    try:
        # 模拟从数据库获取审计日志
        # 在实际应用中，这里应该从Doris的审计日志表中获取数据
        
        audit_logs = [
            {
                "timestamp": "2023-07-01 12:34:56",
                "user": "admin",
                "operation": "SELECT",
                "database": "example_db",
                "table": "orders",
                "status": "成功",
                "duration": 0.123
            },
            {
                "timestamp": "2023-07-01 12:35:22",
                "user": "user1",
                "operation": "INSERT",
                "database": "example_db",
                "table": "customers",
                "status": "成功",
                "duration": 0.056
            },
            {
                "timestamp": "2023-07-01 12:40:15",
                "user": "user2",
                "operation": "UPDATE",
                "database": "example_db",
                "table": "products",
                "status": "失败",
                "duration": 0.098
            }
        ]
        
        # 构建HTML内容
        html = "<h1>审计日志</h1>"
        
        if not audit_logs:
            html += "<p>没有可用的审计日志</p>"
            return html
        
        html += "<table border='1'>"
        html += "<tr><th>时间</th><th>用户</th><th>操作</th><th>数据库</th><th>表</th><th>状态</th><th>耗时(秒)</th></tr>"
        
        for log in audit_logs:
            # 根据状态设置不同的颜色
            row_style = ""
            if log["status"] == "失败":
                row_style = "style='background-color: #ffeeee;'"
            
            html += f"<tr {row_style}>"
            html += f"<td>{log['timestamp']}</td>"
            html += f"<td>{log['user']}</td>"
            html += f"<td>{log['operation']}</td>"
            html += f"<td>{log['database']}</td>"
            html += f"<td>{log['table']}</td>"
            html += f"<td>{log['status']}</td>"
            html += f"<td>{log['duration']}</td>"
            html += "</tr>"
        
        html += "</table>"
        
        logger.info("成功获取审计日志")
        return html
    except Exception as e:
        logger.error(f"获取审计日志时出错: {str(e)}")
        return f"<h1>Error</h1><p>获取审计日志时出错: {str(e)}</p>" 