#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PostgreSQL数据库监控工具
使用Rich库提供丰富的终端显示效果
"""

import time
import signal
import sys
import re
import threading
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from configparser import ConfigParser
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
import pymysql
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.live import Live
from rich.text import Text
from rich.columns import Columns
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich import box
from rich.align import Align


@dataclass
class DatabaseConfig:
    """数据库配置"""
    host: str
    port: int
    database: str
    username: str
    password: str


@dataclass
class MySQLConfig(DatabaseConfig):
    """MySQL配置"""
    databases: List[str] = field(default_factory=list)
    ignored_prefixes: List[str] = field(default_factory=list)


@dataclass
class TableInfo:
    """表信息"""
    schema_name: str
    target_table_name: str  # PostgreSQL中的目标表名
    pg_rows: int = 0
    mysql_rows: int = 0
    previous_pg_rows: int = 0
    mysql_source_tables: List[str] = field(default_factory=list)
    last_updated: datetime = field(default_factory=datetime.now)  # 改为datetime对象
    mysql_last_updated: datetime = field(default_factory=datetime.now)  # MySQL数据最后更新时间
    is_first_query: bool = True
    mysql_updating: bool = False  # MySQL是否正在更新中
    pg_is_estimated: bool = False  # PG数据是否为估计值
    mysql_is_estimated: bool = False  # MySQL数据是否为估计值

    @property
    def change(self) -> int:
        """PostgreSQL记录数变化"""
        return 0 if self.is_first_query else self.pg_rows - self.previous_pg_rows

    @property
    def data_diff(self) -> int:
        """数据差异"""
        if self.pg_rows == -1 or self.mysql_rows == -1:
            return 0  # 错误状态时差异为0，避免统计计算错误
        return self.pg_rows - self.mysql_rows

    @property
    def is_consistent(self) -> bool:
        """数据是否一致"""
        if self.pg_rows == -1 or self.mysql_rows == -1:
            return False  # 错误状态视为不一致
        return self.pg_rows == self.mysql_rows

    @property
    def full_name(self) -> str:
        """完整表名"""
        return f"{self.schema_name}.{self.target_table_name}"


class SyncProperties:
    """表名映射规则（与Java版本保持一致）"""

    @staticmethod
    def get_target_table_name(source_table_name: str) -> str:
        """
        生成目标表名
        应用表名映射规则：table_runtime、table_uuid、table_数字 统一映射到 table
        """
        if not source_table_name or not source_table_name.strip():
            return source_table_name

        # 检查是否包含下划线
        if '_' not in source_table_name:
            return source_table_name  # 没有下划线，直接返回

        # 1. 检查 runtime 后缀
        if source_table_name.endswith('_runtime'):
            return source_table_name[:-8]  # 移除 "_runtime"

        # 2. 检查 9位数字后缀
        last_underscore_index = source_table_name.rfind('_')
        if last_underscore_index > 0:
            suffix = source_table_name[last_underscore_index + 1:]
            if SyncProperties._is_numeric_suffix(suffix):
                return source_table_name[:last_underscore_index]

        # 2a. 检查 9位数字_年度 格式
        # 例如: order_bom_item_333367878_2018
        if re.match(r'.*_\d{9}_\d{4}$', source_table_name):
            return re.sub(r'_\d{9}_\d{4}$', '', source_table_name)

        # 3. 检查各种UUID格式后缀
        extracted_base_name = SyncProperties._extract_table_name_from_uuid(source_table_name)
        if extracted_base_name != source_table_name:
            return extracted_base_name

        # 不符合映射规则，保持原样
        return source_table_name

    @staticmethod
    def _is_numeric_suffix(s: str) -> bool:
        """检查字符串是否为9位纯数字"""
        if not s or not s.strip():
            return False
        return re.match(r'^\d{9}$', s) is not None

    @staticmethod
    def _extract_table_name_from_uuid(table_name: str) -> str:
        """
        从包含UUID的表名中提取基础表名
        支持多种UUID格式：
        1. order_bom_0e9b60a4_d6ed_473d_a326_9e8c8f744ec2 -> order_bom
        2. users_a1b2c3d4-e5f6-7890-abcd-ef1234567890 -> users
        3. products_a1b2c3d4e5f67890abcdef1234567890 -> products
        """
        if not table_name or '_' not in table_name:
            return table_name

        # 模式1: 下划线分隔的UUID格式 (8_4_4_4_12)
        # 例如: order_bom_0e9b60a4_d6ed_473d_a326_9e8c8f744ec2
        pattern1 = r'_[0-9a-fA-F]{8}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{12}$'
        if re.search(pattern1, table_name):
            return re.sub(pattern1, '', table_name)

        # 模式2: 连字符分隔的UUID格式 (8-4-4-4-12)
        # 例如: users_a1b2c3d4-e5f6-7890-abcd-ef1234567890
        pattern2 = r'_[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        if re.search(pattern2, table_name):
            return re.sub(pattern2, '', table_name)

        # 模式3: 下划线分隔的UUID格式后跟年度 (8_4_4_4_12_年度)
        # 例如: order_bom_item_05355967_c503_4a2d_9dd1_2dd7a9ffa15e_2030
        pattern3 = r'_[0-9a-fA-F]{8}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{12}_\d{4}$'
        if re.search(pattern3, table_name):
            return re.sub(pattern3, '', table_name)

        # 模式4: 混合格式 - 移除所有分隔符后检查是否为32位十六进制
        parts = table_name.split('_')
        if len(parts) >= 2:
            # 从后往前组合，找到可能的UUID开始位置
            for i in range(len(parts) - 1, 0, -1):
                possible_uuid_parts = parts[i:]
                possible_uuid = '_'.join(possible_uuid_parts)
                clean_uuid = re.sub(r'[-_]', '', possible_uuid)

                if len(clean_uuid) == 32 and re.match(r'^[0-9a-fA-F]{32}$', clean_uuid):
                    # 找到了UUID，返回基础表名
                    return '_'.join(parts[:i])
                elif len(clean_uuid) > 32:
                    break  # 太长了，不可能是UUID

        return table_name  # 没有找到UUID模式，返回原表名


class PostgreSQLMonitor:
    """PostgreSQL监控器"""

    def __init__(self, config_file: str = "config.ini", override_databases: Optional[List[str]] = None):
        self.console = Console()
        self.config_file = config_file
        self.override_databases = override_databases  # 命令行传入的数据库列表
        self.pg_config = None
        self.mysql_config = None
        self.monitor_config = {}
        self.tables: List[TableInfo] = []
        self.iteration = 0
        self.sync_props = SyncProperties()
        self.start_time = datetime.now()  # 程序启动时间

        # 分离的更新计数器
        self.pg_iteration = 0  # PostgreSQL更新次数
        self.mysql_iteration = 0  # MySQL更新次数
        self.mysql_update_interval = 3  # MySQL更新间隔（相对于PostgreSQL）
        self.first_mysql_update = True  # 标记是否是第一次MySQL更新
        self.first_pg_update = True  # 标记是否是第一次PostgreSQL更新
        
        # 停止标志，用于优雅退出
        self.stop_event = threading.Event()
        
        # 异步MySQL更新支持
        self.mysql_executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="mysql-worker")
        self.mysql_update_lock = threading.Lock()
        self.mysql_update_futures = []  # 跟踪正在进行的MySQL更新任务

        # 信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def get_relative_time(self, target_time: datetime) -> str:
        """获取相对时间显示"""
        now = datetime.now()
        diff = now - target_time

        # 计算总秒数
        total_seconds = int(diff.total_seconds())

        if total_seconds < 0:
            return "刚刚"
        elif total_seconds < 60:
            return f"{total_seconds}秒前"
        elif total_seconds < 3600:  # 小于1小时
            minutes = total_seconds // 60
            return f"{minutes}分钟前"
        elif total_seconds < 86400:  # 小于1天
            hours = total_seconds // 3600
            return f"{hours}小时前"
        elif total_seconds < 2592000:  # 小于30天
            days = total_seconds // 86400
            return f"{days}天前"
        elif total_seconds < 31536000:  # 小于1年
            months = total_seconds // 2592000
            return f"{months}个月前"
        else:
            years = total_seconds // 31536000
            return f"{years}年前"

    def _signal_handler(self, signum, frame):
        """信号处理器 - 快速响应，不等待长时间任务"""
        self.console.print("\n[yellow]正在停止监控程序...[/yellow]")
        
        # 设置停止标志
        self.stop_event.set()
        
        # 立即关闭线程池，不等待正在执行的任务完成
        if hasattr(self, 'mysql_executor'):
            self.console.print("[dim]强制关闭MySQL查询线程...[/dim]")
            # 使用shutdown(wait=False)立即关闭，不等待正在执行的任务
            self.mysql_executor.shutdown(wait=False)
            
            # 可选：尝试取消正在进行的任务
            for future in self.mysql_update_futures:
                if not future.done():
                    future.cancel()
        
        self.console.print("[yellow]监控程序已停止[/yellow]")
        sys.exit(0)

    def load_config(self) -> bool:
        """加载配置文件"""
        config_path = Path(self.config_file)
        if not config_path.exists():
            self.console.print(f"[red]配置文件不存在: {config_path}[/red]")
            return False

        try:
            config = ConfigParser()
            config.read(config_path, encoding='utf-8')

            # PostgreSQL配置
            pg_section = config['postgresql']
            self.pg_config = DatabaseConfig(
                host=pg_section['host'],
                port=int(pg_section['port']),
                database=pg_section['database'],
                username=pg_section['username'],
                password=pg_section['password']
            )

            # MySQL配置
            mysql_section = config['mysql']
            
            # 如果有命令行传入的数据库列表，使用它覆盖配置文件
            if self.override_databases:
                databases_list = self.override_databases
                self.console.print(f"[yellow]使用命令行指定的数据库: {', '.join(databases_list)}[/yellow]")
            else:
                databases_list = mysql_section['databases'].split(',')
                self.console.print(f"[yellow]使用配置文件中的数据库: {', '.join(databases_list)}[/yellow]")
            
            self.mysql_config = MySQLConfig(
                host=mysql_section['host'],
                port=int(mysql_section['port']),
                database="",  # 会动态切换
                username=mysql_section['username'],
                password=mysql_section['password'],
                databases=databases_list,
                ignored_prefixes=mysql_section.get('ignored_table_prefixes', '').split(',')
            )

            # 监控配置
            monitor_section = config['monitor']
            self.monitor_config = {
                'refresh_interval': int(monitor_section.get('refresh_interval', 3)),
                'max_tables_display': int(monitor_section.get('max_tables_display', 50)),
                'enable_clear_screen': monitor_section.getboolean('enable_clear_screen', True),
                'mysql_update_interval': int(monitor_section.get('mysql_update_interval', 3))
            }

            # 更新MySQL更新间隔
            self.mysql_update_interval = self.monitor_config['mysql_update_interval']

            return True

        except Exception as e:
            self.console.print(f"[red]配置文件加载失败: {e}[/red]")
            return False

    def connect_postgresql(self) -> Optional[psycopg2.extensions.connection]:
        """连接PostgreSQL"""
        try:
            conn = psycopg2.connect(
                host=self.pg_config.host,
                port=self.pg_config.port,
                database=self.pg_config.database,
                user=self.pg_config.username,
                password=self.pg_config.password,
                connect_timeout=10
            )
            return conn
        except Exception as e:
            self.console.print(f"[red]PostgreSQL连接失败: {e}[/red]")
            return None

    def connect_mysql(self, database: str) -> Optional[pymysql.Connection]:
        """连接MySQL - 使用更短的超时时间"""
        try:
            conn = pymysql.connect(
                host=self.mysql_config.host,
                port=self.mysql_config.port,
                database=database,
                user=self.mysql_config.username,
                password=self.mysql_config.password,
                connect_timeout=5,    # 减少连接超时时间：10秒 -> 5秒
                read_timeout=15,      # 减少读取超时时间：30秒 -> 15秒  
                write_timeout=15,     # 减少写入超时时间：30秒 -> 15秒
                charset='utf8mb4'
            )
            return conn
        except Exception as e:
            self.console.print(f"[red]MySQL连接失败 ({database}): {e}[/red]")
            return None

    def initialize_tables_from_mysql(self) -> Dict[str, Dict[str, TableInfo]]:
        """
        从MySQL初始化表结构（不获取count）
        返回: {schema_name: {target_table_name: TableInfo}}
        """
        schema_tables = {}
        total_databases = len([db for db in self.mysql_config.databases if db.strip()])

        for i, schema_name in enumerate(self.mysql_config.databases, 1):
            schema_name = schema_name.strip()
            if not schema_name:
                continue

            # 显示当前处理的数据库进度
            self.console.print(f"[dim]正在处理数据库 {i}/{total_databases}: {schema_name}[/dim]")
            
            mysql_conn = self.connect_mysql(schema_name)
            if not mysql_conn:
                self.console.print(f"[red]  ❌ 连接失败: {schema_name}[/red]")
                continue

            try:
                # 获取MySQL表列表
                with mysql_conn.cursor() as cursor:
                    cursor.execute("""
                                   SELECT TABLE_NAME
                                   FROM INFORMATION_SCHEMA.TABLES
                                   WHERE TABLE_SCHEMA = %s
                                     AND TABLE_TYPE = 'BASE TABLE'
                                   """, (schema_name,))

                    mysql_table_names = []
                    for row in cursor.fetchall():
                        table_name = row[0]
                        # 过滤忽略的表
                        if not any(table_name.startswith(prefix.strip())
                                   for prefix in self.mysql_config.ignored_prefixes if prefix.strip()):
                            mysql_table_names.append(table_name)

                # 按目标表名分组
                target_tables = {}
                for mysql_table_name in mysql_table_names:
                    # 计算目标表名
                    target_table_name = self.sync_props.get_target_table_name(mysql_table_name)

                    # 如果目标表已存在，添加源表；否则创建新的TableInfo
                    if target_table_name not in target_tables:
                        current_time = datetime.now()
                        target_tables[target_table_name] = TableInfo(
                            schema_name=schema_name,
                            target_table_name=target_table_name,
                            mysql_rows=0,  # 初始为0，后续更新
                            mysql_last_updated=current_time - timedelta(days=365),  # 设置为很久以前
                            last_updated=current_time
                        )
                        target_tables[target_table_name].mysql_source_tables.append(mysql_table_name)
                    else:
                        target_tables[target_table_name].mysql_source_tables.append(mysql_table_name)

                if target_tables:
                    schema_tables[schema_name] = target_tables
                    self.console.print(f"[green]  ✅ 成功: {schema_name} ({len(target_tables)} 个目标表)[/green]")
                else:
                    self.console.print(f"[yellow]  ⚠️  无表: {schema_name}[/yellow]")

            finally:
                mysql_conn.close()

        return schema_tables

    def get_mysql_rows_from_information_schema(self, target_tables: Dict[str, Dict[str, TableInfo]]):
        """第一次运行时使用information_schema快速获取MySQL表行数估计值"""
        current_time = datetime.now()

        for schema_name, tables_dict in target_tables.items():
            mysql_conn = self.connect_mysql(schema_name)
            if not mysql_conn:
                continue

            try:
                with mysql_conn.cursor() as cursor:
                    # 使用information_schema.tables一次性获取所有表的行数估计
                    cursor.execute("""
                        SELECT table_name, table_rows 
                        FROM information_schema.tables 
                        WHERE table_schema = %s 
                        AND table_type = 'BASE TABLE'
                        ORDER BY table_rows DESC
                    """, (schema_name,))
                    
                    # 建立表名到行数的映射
                    table_rows_map = {}
                    for row in cursor.fetchall():
                        table_name, table_rows = row
                        table_rows_map[table_name] = table_rows or 0  # 处理NULL值

                # 更新TableInfo中的MySQL行数
                for table_info in tables_dict.values():
                    table_info.mysql_rows = 0  # 重置
                    
                    # 累加所有源表的估计行数
                    for mysql_table_name in table_info.mysql_source_tables:
                        if mysql_table_name in table_rows_map:
                            table_info.mysql_rows += table_rows_map[mysql_table_name]
                    
                    table_info.mysql_last_updated = current_time
                    table_info.mysql_is_estimated = True  # 标记为估计值

            finally:
                mysql_conn.close()

    def _update_single_schema_mysql(self, schema_name: str, tables_dict: Dict[str, TableInfo], use_information_schema: bool = False) -> bool:
        """更新单个schema的MySQL记录数（线程安全，支持中断）"""
        current_time = datetime.now()
        
        # 检查是否收到停止信号
        if self.stop_event.is_set():
            return False
        
        try:
            mysql_conn = self.connect_mysql(schema_name)
            if not mysql_conn:
                return False

            try:
                if use_information_schema:
                    # 检查停止标志
                    if self.stop_event.is_set():
                        return False
                        
                    # 第一次运行使用information_schema快速获取估计值
                    with mysql_conn.cursor() as cursor:
                        cursor.execute("""
                            SELECT table_name, table_rows 
                            FROM information_schema.tables 
                            WHERE table_schema = %s 
                            AND table_type = 'BASE TABLE'
                            ORDER BY table_rows DESC
                        """, (schema_name,))
                        
                        # 建立表名到行数的映射
                        table_rows_map = {}
                        for row in cursor.fetchall():
                            table_name, table_rows = row
                            table_rows_map[table_name] = table_rows or 0  # 处理NULL值

                    # 更新TableInfo中的MySQL行数
                    for table_info in tables_dict.values():
                        # 检查停止标志
                        if self.stop_event.is_set():
                            return False
                            
                        with self.mysql_update_lock:
                            if table_info.mysql_updating:
                                continue  # 如果正在更新中，跳过

                            table_info.mysql_updating = True
                            table_info.mysql_rows = 0  # 重置
                            
                            # 累加所有源表的估计行数
                            for mysql_table_name in table_info.mysql_source_tables:
                                if mysql_table_name in table_rows_map:
                                    table_info.mysql_rows += table_rows_map[mysql_table_name]
                            
                            table_info.mysql_last_updated = current_time
                            table_info.mysql_updating = False
                            table_info.mysql_is_estimated = True  # 标记为估计值
                else:
                    # 常规更新使用精确的COUNT查询
                    for table_info in tables_dict.values():
                        # 检查停止标志
                        if self.stop_event.is_set():
                            return False
                            
                        with self.mysql_update_lock:
                            if table_info.mysql_updating:
                                continue  # 如果正在更新中，跳过
                            table_info.mysql_updating = True
                            
                        # 在锁外执行查询以避免长时间锁定
                        temp_mysql_rows = 0
                        
                        # 更新所有源表的记录数
                        for mysql_table_name in table_info.mysql_source_tables:
                            # 检查停止标志
                            if self.stop_event.is_set():
                                with self.mysql_update_lock:
                                    table_info.mysql_updating = False
                                return False
                                
                            try:
                                with mysql_conn.cursor() as cursor:
                                    # 先尝试使用主键索引进行count查询
                                    try:
                                        cursor.execute(f"SELECT COUNT(*) FROM `{mysql_table_name}` USE INDEX (PRIMARY)")
                                        mysql_rows = cursor.fetchone()[0]
                                    except Exception:
                                        # 如果使用索引失败（可能没有主键索引），使用普通查询
                                        cursor.execute(f"SELECT COUNT(*) FROM `{mysql_table_name}`")
                                        mysql_rows = cursor.fetchone()[0]
                                temp_mysql_rows += mysql_rows
                            except Exception as e:
                                # 表可能不存在或无权限，跳过
                                continue
                        
                        # 查询完成后更新结果
                        with self.mysql_update_lock:
                            table_info.mysql_rows = temp_mysql_rows
                            table_info.mysql_last_updated = current_time
                            table_info.mysql_updating = False
                            table_info.mysql_is_estimated = False  # 标记为精确值

                return True
            finally:
                mysql_conn.close()
                
        except Exception as e:
            # 出现异常时，标记所有表的mysql_updating为False
            with self.mysql_update_lock:
                for table_info in tables_dict.values():
                    if table_info.mysql_updating:
                        table_info.mysql_updating = False
            return False

    def update_mysql_counts_async(self, target_tables: Dict[str, Dict[str, TableInfo]], use_information_schema: bool = False):
        """异步更新MySQL记录数（不阻塞主线程）"""
        # 清理已完成的任务
        self.mysql_update_futures = [f for f in self.mysql_update_futures if not f.done()]
        
        # 为每个schema提交异步更新任务
        for schema_name, tables_dict in target_tables.items():
            # 检查该schema是否已经有正在进行的更新任务
            schema_updating = False
            with self.mysql_update_lock:
                for table_info in tables_dict.values():
                    if table_info.mysql_updating:
                        schema_updating = True
                        break
            
            if not schema_updating:
                future = self.mysql_executor.submit(self._update_single_schema_mysql, schema_name, tables_dict, use_information_schema)
                self.mysql_update_futures.append(future)

    def update_mysql_counts(self, target_tables: Dict[str, Dict[str, TableInfo]], use_information_schema: bool = False):
        """更新MySQL记录数（同步版本，用于兼容性）"""
        for schema_name, tables_dict in target_tables.items():
            self._update_single_schema_mysql(schema_name, tables_dict, use_information_schema)

    def get_postgresql_rows_from_pg_stat(self, conn, target_tables: Dict[str, Dict[str, TableInfo]]):
        """第一次运行时使用pg_stat_user_tables快速获取PostgreSQL表行数估计值"""
        current_time = datetime.now()

        for schema_name, tables_dict in target_tables.items():
            try:
                with conn.cursor() as cursor:
                    # 一次性获取该schema下所有表的统计信息
                    cursor.execute("""
                        SELECT relname, n_tup_ins - n_tup_del + n_tup_upd AS estimated_rows
                        FROM pg_stat_user_tables 
                        WHERE schemaname = %s
                    """, (schema_name,))
                    
                    # 建立表名到估计行数的映射
                    pg_stats_map = {}
                    for row in cursor.fetchall():
                        table_name, estimated_rows = row
                        pg_stats_map[table_name] = max(0, estimated_rows or 0)  # 确保非负数

                # 更新TableInfo
                for target_table_name, table_info in tables_dict.items():
                    if target_table_name in pg_stats_map:
                        new_count = pg_stats_map[target_table_name]
                    else:
                        # 如果统计信息中没有，可能是新表或无数据，使用精确查询
                        try:
                            with conn.cursor() as cursor:
                                cursor.execute(f'SELECT COUNT(*) FROM "{schema_name}"."{target_table_name}"')
                                new_count = cursor.fetchone()[0]
                        except:
                            new_count = -1  # 查询失败标记为-1

                    if not table_info.is_first_query:
                        table_info.previous_pg_rows = table_info.pg_rows
                    else:
                        table_info.previous_pg_rows = new_count
                        table_info.is_first_query = False

                    table_info.pg_rows = new_count
                    table_info.last_updated = current_time
                    table_info.pg_is_estimated = True  # 标记为估计值

            except Exception as e:
                # 如果pg_stat查询失败，回退到逐表精确查询
                self.update_postgresql_counts(conn, {schema_name: tables_dict})

    def update_postgresql_counts(self, conn, target_tables: Dict[str, Dict[str, TableInfo]]):
        """更新PostgreSQL记录数（常规精确查询）"""
        current_time = datetime.now()
        self._update_postgresql_counts_exact(conn, target_tables, current_time)

    def _update_postgresql_counts_exact(self, conn, target_tables: Dict[str, Dict[str, TableInfo]], current_time):
        """使用精确COUNT查询更新PostgreSQL记录数"""
        for schema_name, tables_dict in target_tables.items():
            for target_table_name, table_info in tables_dict.items():
                try:
                    with conn.cursor() as cursor:
                        # 直接获取记录数
                        cursor.execute(f'SELECT COUNT(*) FROM "{schema_name}"."{target_table_name}"')
                        new_count = cursor.fetchone()[0]

                        if not table_info.is_first_query:
                            table_info.previous_pg_rows = table_info.pg_rows
                        else:
                            table_info.previous_pg_rows = new_count
                            table_info.is_first_query = False

                        table_info.pg_rows = new_count
                        table_info.last_updated = current_time
                        table_info.pg_is_estimated = False  # 标记为精确值

                except Exception as e:
                    # 出现异常时标记为错误状态，记录数设为-1表示错误
                    if not table_info.is_first_query:
                        table_info.previous_pg_rows = table_info.pg_rows
                    else:
                        table_info.previous_pg_rows = -1
                        table_info.is_first_query = False
                    
                    table_info.pg_rows = -1  # -1表示查询失败
                    table_info.last_updated = current_time
                    table_info.pg_is_estimated = False  # 错误状态不是估计值

    def create_header_panel(self) -> Panel:
        """创建标题面板"""
        title_text = Text()
        title_text.append("🔍 PostgreSQL 数据库监控", style="bold cyan")
        title_text.append(f" - PG第{self.pg_iteration}次/MySQL第{self.mysql_iteration}次", style="dim")

        time_text = Text()
        time_text.append("⏰ 运行时长: ", style="dim")
        runtime_text = self.get_relative_time(self.start_time).rstrip("前")
        time_text.append(runtime_text, style="green")

        header_content = Align.center(Columns([title_text, time_text], equal=True))
        return Panel(header_content, box=box.ROUNDED, style="blue")

    def create_combined_stats_panel(self, tables: List[TableInfo]) -> Panel:
        """创建合并的统计面板"""
        # 过滤掉错误状态的表进行统计
        valid_tables = [t for t in tables if t.pg_rows != -1 and t.mysql_rows != -1]
        error_tables = [t for t in tables if t.pg_rows == -1 or t.mysql_rows == -1]
        
        total_pg_rows = sum(t.pg_rows for t in valid_tables)
        total_mysql_rows = sum(t.mysql_rows for t in valid_tables)
        total_diff = total_pg_rows - total_mysql_rows
        total_changes = sum(t.change for t in valid_tables)
        changed_count = len([t for t in valid_tables if t.change != 0])

        # 按schema分组统计
        schema_stats = {}
        for table in tables:
            if table.schema_name not in schema_stats:
                schema_stats[table.schema_name] = {
                    'count': 0, 'pg_rows': 0, 'mysql_rows': 0,
                    'changes': 0, 'inconsistent': 0
                }

            stats = schema_stats[table.schema_name]
            stats['count'] += 1
            stats['pg_rows'] += table.pg_rows
            stats['mysql_rows'] += table.mysql_rows
            stats['changes'] += table.change
            if not table.is_consistent:
                stats['inconsistent'] += 1

        # 一致性统计
        consistent_count = len([t for t in tables if t.is_consistent])
        inconsistent_count = len(tables) - consistent_count

        # 统一的统计信息
        stats_text = Text()
        stats_text.append("📊 监控统计: ", style="bold")
        
        # 第一行：基本统计和Schema信息
        stats_text.append(f"{len(tables)} 个表", style="white")
        
        # 始终显示Schema详细信息
        if schema_stats:
            stats_text.append(f", {len(schema_stats)} 个 Schema: ", style="white")
            schema_names = sorted(schema_stats.keys())
            for i, schema_name in enumerate(schema_names):
                if i > 0:
                    stats_text.append(", ", style="white")
                stats_text.append(f"{schema_name}", style="cyan")
                # 显示每个schema的表数量
                stats_text.append(f"({schema_stats[schema_name]['count']})", style="dim")
        else:
            stats_text.append(" (无Schema)", style="red")
        stats_text.append("\n")
        
        # 第二行：数据量统计
        stats_text.append(f"📈 PG总计: {total_pg_rows:,} 行, ", style="white")
        stats_text.append(f"MySQL总计: {total_mysql_rows:,} 行, ", style="white")
        
        diff_style = "red" if total_diff < 0 else "green" if total_diff > 0 else "white"
        stats_text.append(f"数据差异: {total_diff:+,} 行\n", style=diff_style)
        
        # 第三行：变化和一致性统计
        change_style = "green" if total_changes > 0 else "red" if total_changes < 0 else "white"
        stats_text.append(f"🔄 本轮变化: {total_changes:+,} 行 ({changed_count} 个表有变化), ", style=change_style)
        
        stats_text.append(f"一致性: ", style="white")
        stats_text.append(f"{consistent_count} 个一致", style="green")
        if inconsistent_count > 0:
            stats_text.append(f", {inconsistent_count} 个不一致", style="red")
        if len(error_tables) > 0:
            stats_text.append(f", {len(error_tables)} 个错误", style="red")
        
        # 显示MySQL异步任务状态
        mysql_updating_count = sum(1 for t in tables if t.mysql_updating)
        active_futures = len([f for f in self.mysql_update_futures if not f.done()])
        if mysql_updating_count > 0 or active_futures > 0:
            stats_text.append(f", MySQL更新中: {mysql_updating_count} 个表, {active_futures} 个任务", style="yellow")
        
        # 如果有多个Schema，显示详细的Schema统计
        if len(schema_stats) > 1:
            stats_text.append("\n📋 Schema详情: ", style="bold")
            for i, (schema_name, stats) in enumerate(sorted(schema_stats.items())):
                if i > 0:
                    stats_text.append(" | ", style="dim")
                
                # Schema名称和基本信息
                stats_text.append(f"{schema_name}: ", style="cyan")
                stats_text.append(f"{stats['count']}表 ", style="white")
                
                # 数据差异
                schema_diff = stats['pg_rows'] - stats['mysql_rows']
                diff_style = "red" if schema_diff < 0 else "green" if schema_diff > 0 else "white"
                stats_text.append(f"差异{schema_diff:+,} ", style=diff_style)
                
                # 不一致表数量
                if stats['inconsistent'] > 0:
                    stats_text.append(f"({stats['inconsistent']}不一致)", style="red")

        combined_content = stats_text
        return Panel(combined_content, title="监控统计", box=box.ROUNDED, style="green")

    def create_tables_table(self, tables: List[TableInfo]) -> Table:
        """创建表格"""

        # 智能排序：数据不一致的表优先显示，一致的表排在后面
        def sort_key(t: TableInfo):
            # 排序优先级：
            # 1. 数据不一致的表（最高优先级）- 按数据差异绝对值降序
            # 2. 数据一致但有变化的表 - 按变化量绝对值降序  
            # 3. 数据一致且无变化的表 - 按PostgreSQL记录数降序
            if not t.is_consistent:
                return (0, -abs(t.data_diff), -t.pg_rows)  # 数据不一致，按差异绝对值和记录数排序
            elif t.change != 0:
                return (1, -abs(t.change), -t.pg_rows)  # 数据一致但有变化，按变化量和记录数排序
            else:
                return (2, -t.pg_rows)  # 数据一致且无变化，按记录数排序

        sorted_tables = sorted(tables, key=sort_key)
        max_display = self.monitor_config['max_tables_display']
        display_tables = sorted_tables[:max_display]

        table = Table(box=box.ROUNDED, show_header=True, header_style="bold magenta")
        table.add_column("序号", justify="right", style="dim", width=4)
        table.add_column("状态", justify="center", width=4)
        table.add_column("SCHEMA", style="cyan", width=15)
        table.add_column("目标表名", style="blue", width=18)
        table.add_column("PG记录数", justify="right", style="white", width=12)
        table.add_column("MySQL汇总数", justify="right", style="white", width=12)
        table.add_column("数据差异", justify="right", width=10)
        table.add_column("变化量", justify="center", width=12)
        table.add_column("PG更新时间", justify="center", style="dim", width=10)
        table.add_column("MySQL状态", justify="center", style="dim", width=12)
        table.add_column("源表数量", style="dim", width=8)

        for i, t in enumerate(display_tables, 1):

            # 图标选择 - 包含错误状态
            if t.pg_rows == -1 or t.mysql_rows == -1:
                icon = "❌"  # 查询错误
            elif t.is_consistent:
                icon = "✅"  # 数据一致
            else:
                icon = "⚠️"  # 数据不一致

            # 截断长名称
            schema_name = t.schema_name[:13] + "..." if len(t.schema_name) > 15 else t.schema_name
            # 现在图标单独一列，表名可以显示更多字符
            table_name = t.target_table_name[:15] + "..." if len(t.target_table_name) > 18 else t.target_table_name

            # 数据差异样式 - 处理错误状态
            if t.pg_rows == -1 or t.mysql_rows == -1:
                diff_style = "red"
                diff_text = "ERROR"
            else:
                diff_style = "red" if t.data_diff < 0 else "green" if t.data_diff > 0 else "white"
                diff_text = f"{t.data_diff:+,}" if t.data_diff != 0 else "0"

            # 变化量样式 - 处理错误状态
            if t.pg_rows == -1:
                change_text = "ERROR"
                change_style = "red"
            elif t.change > 0:
                change_text = f"+{t.change:,} ⬆"
                change_style = "green"
            elif t.change < 0:
                change_text = f"{t.change:,} ⬇"
                change_style = "red"
            else:
                change_text = "0 ─"
                change_style = "white"

            # MySQL源表数量显示
            source_count = len(t.mysql_source_tables)
            source_display = str(source_count)

            # MySQL状态显示
            if t.mysql_updating:
                mysql_status = "[yellow]更新中[/yellow]"
            else:
                mysql_relative_time = self.get_relative_time(t.mysql_last_updated)
                if "年前" in mysql_relative_time or "个月前" in mysql_relative_time:
                    mysql_status = "[red]未更新[/red]"
                else:
                    mysql_status = f"[green]{mysql_relative_time}[/green]"

            # 处理记录数显示
            if t.pg_rows == -1:
                pg_rows_display = "ERROR"
            elif t.pg_is_estimated:
                pg_rows_display = f"~{t.pg_rows:,}"
            else:
                pg_rows_display = f"{t.pg_rows:,}"
                
            if t.mysql_rows == -1:
                mysql_rows_display = "ERROR"
            elif t.mysql_is_estimated:
                mysql_rows_display = f"~{t.mysql_rows:,}"
            else:
                mysql_rows_display = f"{t.mysql_rows:,}"
            
            table.add_row(
                str(i),
                icon,
                schema_name,
                table_name,
                f"[red]{pg_rows_display}[/red]" if t.pg_rows == -1 else pg_rows_display,
                f"[red]{mysql_rows_display}[/red]" if t.mysql_rows == -1 else mysql_rows_display,
                f"[{diff_style}]{diff_text}[/{diff_style}]",
                f"[{change_style}]{change_text}[/{change_style}]",
                self.get_relative_time(t.last_updated),
                mysql_status,
                source_display
            )

        return table

    def create_footer_panel(self, tables: List[TableInfo]) -> Panel:
        """创建底部面板"""
        consistent_count = len([t for t in tables if t.is_consistent])
        inconsistent_count = len(tables) - consistent_count
        max_display = self.monitor_config['max_tables_display']

        footer_text = Text()
        footer_text.append("🔍 数据一致性: ", style="bold")
        footer_text.append(f"{consistent_count} 个表一致, ", style="green")
        footer_text.append(f"{inconsistent_count} 个表不一致 ", style="red")
        footer_text.append(f"(显示前 {min(len(tables), max_display)}/{len(tables)} 个表)\n", style="dim")
        footer_text.append("📋 图例: ✅数据一致 ⚠️数据不一致 ❌查询错误 | ~估计值\n", style="dim")
        footer_text.append("🔄 MySQL状态: ", style="dim")
        footer_text.append("[green]绿色=已更新[/green] [yellow]黄色=更新中[/yellow] [red]红色=未更新[/red] | ",
                           style="dim")
        footer_text.append("💡 按 Ctrl+C 停止监控", style="yellow")

        return Panel(footer_text, box=box.ROUNDED, style="dim")

    def create_layout(self, tables: List[TableInfo]) -> Layout:
        """创建布局"""
        layout = Layout()

        layout.split_column(
            Layout(self.create_header_panel(), size=3),
            Layout(self.create_combined_stats_panel(tables), size=6),
            Layout(self.create_tables_table(tables), name="tables"),
            Layout(self.create_footer_panel(tables), size=4)
        )

        return layout

    def run(self):
        """运行监控"""
        if not self.load_config():
            return

        self.console.print("[green]正在启动PostgreSQL监控程序...[/green]")

        # 初始化数据库连接测试
        pg_conn = self.connect_postgresql()
        if not pg_conn:
            return
        pg_conn.close()

        self.console.print(f"[green]配置的MySQL数据库: {', '.join(self.mysql_config.databases)}[/green]")
        self.console.print(f"[green]开始监控，PG刷新间隔: {self.monitor_config['refresh_interval']} 秒[/green]")
        self.console.print(f"[green]MySQL更新间隔: {self.mysql_update_interval} 次PG更新 (异步执行，不阻塞PG查询)[/green]")

        # 初始化表结构 - 显示进度提示
        self.console.print("[yellow]正在初始化表结构，请稍候...[/yellow]")
        
        with self.console.status("[bold green]正在从MySQL获取表信息...") as status:
            target_tables = self.initialize_tables_from_mysql()
            
        # 显示初始化结果
        total_tables = sum(len(tables_dict) for tables_dict in target_tables.values())
        if total_tables > 0:
            self.console.print(f"[green]✅ 初始化完成！发现 {total_tables} 个目标表[/green]")
        else:
            self.console.print("[red]❌ 未发现任何表，请检查配置[/red]")
            return

        # 立即进行第一次数据更新（使用快速查询）
        self.console.print("[yellow]正在进行首次数据更新（MySQL使用information_schema，PostgreSQL使用pg_stat）...[/yellow]")
        
        # 第一次PostgreSQL更新
        pg_conn = self.connect_postgresql()
        if pg_conn:
            self.get_postgresql_rows_from_pg_stat(pg_conn, target_tables)
            pg_conn.close()
            self.first_pg_update = False
        
        # 第一次MySQL更新
        self.mysql_iteration += 1
        self.update_mysql_counts(target_tables, use_information_schema=True)
        self.first_mysql_update = False
        
        self.console.print(f"[green]✅ 首次数据更新完成 (MySQL第{self.mysql_iteration}次)[/green]")
        
        # 给用户3秒时间查看初始化信息
        time.sleep(3)

        # 主监控循环
        try:
            with Live(console=self.console, refresh_per_second=1, screen=True) as live:
                while not self.stop_event.is_set():
                    try:
                        self.iteration += 1

                        # 1. 更新PostgreSQL记录数（每次都更新）
                        self.pg_iteration += 1
                        pg_conn = self.connect_postgresql()
                        if pg_conn:
                            # 后续都使用精确的COUNT查询（首次已经在初始化时完成）
                            self.update_postgresql_counts(pg_conn, target_tables)
                            pg_conn.close()

                        # 2. 按间隔更新MySQL记录数（异步，不阻塞PostgreSQL查询）
                        if self.pg_iteration % self.mysql_update_interval == 0:
                            self.mysql_iteration += 1
                            # 使用异步更新，不阻塞主循环
                            self.update_mysql_counts_async(target_tables, use_information_schema=False)

                        # 3. 将结果转换为列表格式用于显示
                        self.tables = []
                        for schema_name, tables_dict in target_tables.items():
                            for table_info in tables_dict.values():
                                self.tables.append(table_info)

                        # 4. 更新显示
                        live.update(self.create_layout(self.tables))

                        # 等待下次刷新（可被中断）
                        for _ in range(self.monitor_config['refresh_interval']):
                            if self.stop_event.is_set():
                                break
                            time.sleep(1)

                    except KeyboardInterrupt:
                        # 在循环中捕获KeyboardInterrupt，确保能够退出
                        break
                    except Exception as e:
                        if not self.stop_event.is_set():
                            self.console.print(f"[red]监控过程中出错: {e}[/red]")
                            time.sleep(5)
                
        finally:
            # 确保线程池被关闭
            self.console.print("[dim]正在清理资源...[/dim]")
            if hasattr(self, 'mysql_executor'):
                # 再次尝试关闭线程池，这次等待最多2秒
                try:
                    self.mysql_executor.shutdown(wait=False)
                    # 给线程池2秒时间优雅关闭
                    time.sleep(2)
                except:
                    pass
            self.console.print("[green]资源清理完成[/green]")


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(
        description="PostgreSQL数据库监控工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  python3 cdc_monitor.py                          # 使用配置文件中的数据库列表
  python3 cdc_monitor.py --databases db1,db2     # 监控指定的数据库
  python3 cdc_monitor.py -d test_db               # 只监控test_db数据库
  python3 cdc_monitor.py --config my_config.ini  # 使用指定的配置文件
        """
    )
    
    parser.add_argument(
        '--databases', '-d',
        type=str,
        help='指定要监控的MySQL数据库列表（逗号分隔），覆盖配置文件中的databases配置'
    )
    
    parser.add_argument(
        '--config', '-c',
        type=str,
        default="config.ini",
        help='指定配置文件路径（默认: config.ini）'
    )
    
    args = parser.parse_args()
    
    # 检查配置文件是否存在
    config_file = args.config
    if not Path(config_file).exists():
        print(f"❌ 配置文件不存在: {config_file}")
        print("请确保config.ini文件存在并配置正确")
        sys.exit(1)

    # 处理数据库列表参数
    override_databases = None
    if args.databases:
        # 去除空格并分割数据库名称
        override_databases = [db.strip() for db in args.databases.split(',') if db.strip()]
        if not override_databases:
            print("❌ 指定的数据库列表为空")
            sys.exit(1)

    monitor = PostgreSQLMonitor(config_file, override_databases)
    monitor.run()


if __name__ == "__main__":
    main() 