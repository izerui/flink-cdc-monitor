#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PostgreSQL数据库监控工具 - Textual版本
使用Textual框架提供现代化的TUI界面，支持DataTable滚动查看
"""

import argparse
import asyncio
import re
import signal
import sys
from configparser import ConfigParser
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import aiomysql
import asyncpg
from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Container, Vertical
from textual.timer import Timer
from textual.widgets import DataTable, Footer, Header, Static


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
    pg_updating: bool = False  # PostgreSQL是否正在更新中
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


class StatsWidget(Static):
    """统计信息组件"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
    def update_stats(self, tables: List[TableInfo], pg_iteration: int, mysql_iteration: int, start_time: datetime, 
                    is_paused: bool = False, sort_by: str = "schema_table", filter_mode: str = "all"):
        """更新统计数据"""
        # 过滤掉错误状态的表进行统计
        valid_tables = [t for t in tables if t.pg_rows != -1 and t.mysql_rows != -1]
        error_tables = [t for t in tables if t.pg_rows == -1 or t.mysql_rows == -1]

        total_pg_rows = sum(t.pg_rows for t in valid_tables)
        total_mysql_rows = sum(t.mysql_rows for t in valid_tables)
        total_diff = total_pg_rows - total_mysql_rows
        total_changes = sum(t.change for t in valid_tables)
        changed_count = len([t for t in valid_tables if t.change != 0])

        # 一致性统计
        consistent_count = len([t for t in tables if t.is_consistent])
        inconsistent_count = len(tables) - consistent_count

        # 运行时长
        runtime = datetime.now() - start_time
        runtime_str = self._format_duration(runtime.total_seconds())

        # 构建显示文本
        text = Text()
        
        # 标题行
        text.append("🔍 PostgreSQL 数据库监控", style="bold blue")
        text.append(f" - PG第{pg_iteration}次/MySQL第{mysql_iteration}次", style="dim")
        text.append(f" - 运行时长: {runtime_str}", style="cyan")
        
        # 状态和排序信息
        if is_paused:
            text.append(" - ", style="dim")
            text.append("⏸️ 已暂停", style="bold yellow")
        
        # 排序和过滤信息
        sort_display = {
            "schema_table": "Schema.表名",
            "data_diff": "数据差异", 
            "pg_rows": "PG记录数",
            "mysql_rows": "MySQL记录数"
        }
        filter_display = {
            "all": "全部",
            "inconsistent": "不一致",
            "consistent": "一致", 
            "error": "错误"
        }
        text.append(f" - 排序: {sort_display.get(sort_by, sort_by)}", style="dim")
        text.append(f" - 过滤: {filter_display.get(filter_mode, filter_mode)}", style="dim")
        text.append(f" - 总计: {len(tables)} 个表", style="dim")
        text.append("\n\n")
        
        # 数据量统计
        text.append("📈 数据统计: ", style="bold")
        text.append(f"PG总计: {total_pg_rows:,} 行, ", style="white")
        text.append(f"MySQL总计: {total_mysql_rows:,} 行, ", style="white")
        
        # 数据差异颜色语义化
        if total_diff < 0:
            text.append(f"数据差异: {total_diff:+,} 行", style="bold red")
        elif total_diff > 0:
            text.append(f"数据差异: {total_diff:+,} 行", style="bold green")
        else:
            text.append(f"数据差异: {total_diff:+,} 行", style="white")
        text.append("\n")
        
        # 变化和一致性统计
        if total_changes > 0:
            text.append(f"🔄 本轮变化: +{total_changes:,} 行", style="bold green")
        elif total_changes < 0:
            text.append(f"🔄 本轮变化: {total_changes:+,} 行", style="bold red")
        else:
            text.append(f"🔄 本轮变化: {total_changes:+,} 行", style="white")
        
        text.append(f" ({changed_count} 个表有变化), ", style="white")
        text.append(f"一致性: {consistent_count} 个一致", style="bold green")
        
        if inconsistent_count > 0:
            text.append(f", {inconsistent_count} 个不一致", style="bold red")
        if len(error_tables) > 0:
            text.append(f", {len(error_tables)} 个错误", style="bold red")
        
        text.append("\n")
        
        # 进度信息和同步速度 - 带进度条和速度估算
        if total_mysql_rows > 0:
            completion_rate = min(total_pg_rows / total_mysql_rows, 1.0)
            completion_percent = completion_rate * 100
            
            text.append("📊 同步进度: ", style="bold cyan")
            
            # 创建进度条
            bar_width = 20
            filled_width = int(bar_width * completion_rate)
            empty_width = bar_width - filled_width
            
            # 进度条颜色根据完成率
            if completion_rate >= 0.95:
                bar_color = "bold green"
            elif completion_rate >= 0.8:
                bar_color = "bold yellow"
            else:
                bar_color = "bold red"
            
            # 显示进度条
            text.append("█" * filled_width, style=bar_color)
            text.append("░" * empty_width, style="dim")
            text.append(f" {completion_percent:.1f}%", style="bold white")
            text.append(f" ({total_pg_rows:,}/{total_mysql_rows:,})", style="dim")
            
            if completion_rate >= 1.0:
                text.append(" - 已完成", style="bold green")
            else:
                remaining = total_mysql_rows - total_pg_rows
                text.append(f" - 剩余: {remaining:,} 行", style="dim")
                
                # 计算同步速度和预估时间
                if hasattr(self, 'parent_app') and self.parent_app:
                    speed = self.parent_app.calculate_sync_speed()
                    if speed > 0:
                        text.append(f" - 速度: {speed:.1f} 行/秒", style="bright_blue")
                        estimated_time = self.parent_app.estimate_remaining_time(total_mysql_rows, total_pg_rows, speed)
                        text.append(f" - 预估: {estimated_time}", style="bright_blue")
                    else:
                        text.append(" - 速度: 计算中...", style="dim")
        
        self.update(text)
    
    def _format_duration(self, seconds: float) -> str:
        """格式化时长显示"""
        if seconds < 60:
            return f"{int(seconds)}秒"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes}分{secs}秒"
        elif seconds < 86400:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}小时{minutes}分钟"
        else:
            days = int(seconds // 86400)
            hours = int((seconds % 86400) // 3600)
            return f"{days}天{hours}小时"


class MonitorApp(App[None]):
    """监控应用主类"""
    
    CSS = """
    Screen {
        background: $surface;
    }
    
    .stats {
        height: 10;
        border: solid $primary;
        margin: 1;
        padding: 1;
        background: $surface;
    }
    
    .data-table {
        height: 1fr;
        border: solid $primary;
        margin: 1;
        background: $surface;
    }
    
    .data-table > DataTable {
        background: $surface;
        scrollbar-background: $surface;
        scrollbar-color: $primary;
        scrollbar-corner-color: $surface;
    }
    
    DataTable > .datatable--cursor {
        background: $accent 50%;
    }
    
    DataTable > .datatable--hover {
        background: $primary 20%;
    }
    """
    
    BINDINGS = [
        ("q", "quit", "退出"),
        ("r", "refresh", "手动刷新"),
        ("space", "toggle_pause", "暂停/继续"),
        ("s", "sort_toggle", "切换排序"),
        ("f", "filter_toggle", "切换过滤"),
        ("ctrl+c", "quit", "退出"),
    ]
    
    def __init__(self, config_file: str = "config.ini", override_databases: Optional[List[str]] = None):
        super().__init__()
        self.config_file = config_file
        self.override_databases = override_databases
        self.pg_config = None
        self.mysql_config = None
        self.monitor_config = {}
        self.tables: List[TableInfo] = []
        self.iteration = 0
        self.sync_props = SyncProperties()
        self.start_time = datetime.now()

        # 分离的更新计数器
        self.pg_iteration = 0
        self.mysql_iteration = 0
        self.mysql_update_interval = 3
        self.first_mysql_update = True
        self.first_pg_update = True
        self.pg_updating = False

        # 停止标志，用于优雅退出
        self.stop_event = asyncio.Event()

        # 异步更新支持
        self.mysql_update_lock = asyncio.Lock()
        self.mysql_update_tasks = []
        self.pg_update_lock = asyncio.Lock()
        self.pg_update_tasks = []

        # 进度跟踪
        self.history_data = []
        self.max_history_points = 20

        # 定时器
        self.refresh_timer: Optional[Timer] = None

        # 界面控制属性
        self.is_paused = False
        self.sort_by = "schema_table"  # 可选: schema_table, data_diff, pg_rows, mysql_rows
        self.filter_mode = "all"  # 可选: all, inconsistent, consistent, error

        # 信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def compose(self) -> ComposeResult:
        """构建UI组件"""
        yield Header()
        
        with Vertical():
            # 统计信息面板
            yield StatsWidget(classes="stats")
            
            # 数据表格容器
            with Container(classes="data-table"):
                yield DataTable(id="tables")
        
        yield Footer()
    
    def on_mount(self) -> None:
        """应用启动时的初始化"""
        # 设置数据表格
        table = self.query_one("#tables", DataTable)
        table.add_columns(
            "序号", "状态", "SCHEMA", "目标表名", "PG记录数", 
            "MySQL汇总数", "数据差异", "变化量", "PG更新时间", 
            "MySQL更新时间", "源表数量"
        )
        
        # 启动监控任务
        self.call_later(self.start_monitoring)
        
    async def start_monitoring(self):
        """启动监控任务"""
        if not await self.load_config():
            self.exit(1)
            return
            
        # 初始化数据库连接测试
        pg_conn = await self.connect_postgresql()
        if not pg_conn:
            self.exit(1)
            return
        await pg_conn.close()
        
        # 初始化表结构
        target_tables = await self.initialize_tables_from_mysql()
        total_tables = sum(len(tables_dict) for tables_dict in target_tables.values())
        
        if total_tables == 0:
            self.exit(1)
            return
            
        # 第一次数据更新
        pg_conn = await self.connect_postgresql()
        if pg_conn:
            await self.get_postgresql_rows_from_pg_stat(pg_conn, target_tables)
            await pg_conn.close()
            self.first_pg_update = False
            
        self.mysql_iteration += 1
        await self.update_mysql_counts(target_tables, use_information_schema=True)
        self.first_mysql_update = False
        
        # 转换为列表格式
        self.tables = []
        for schema_name, tables_dict in target_tables.items():
            for table_info in tables_dict.values():
                self.tables.append(table_info)
                
        # 更新显示
        self.update_display()
        
        # 启动定时刷新
        refresh_interval = self.monitor_config.get('refresh_interval', 3)
        self.refresh_timer = self.set_interval(refresh_interval, self.refresh_data)
        
    def update_display(self):
        """更新显示内容"""
        # 更新统计信息
        stats_widget = self.query_one(StatsWidget)
        stats_widget.parent_app = self  # 传递app实例引用
        stats_widget.update_stats(
            self.tables, 
            self.pg_iteration, 
            self.mysql_iteration, 
            self.start_time,
            self.is_paused,
            self.sort_by,
            self.filter_mode
        )
        
        # 更新数据表格
        self._update_data_table()
    
    def _filter_tables(self, tables: List[TableInfo]) -> List[TableInfo]:
        """根据当前过滤模式过滤表格"""
        if self.filter_mode == "inconsistent":
            return [t for t in tables if not t.is_consistent]
        elif self.filter_mode == "consistent":
            return [t for t in tables if t.is_consistent]
        elif self.filter_mode == "error":
            return [t for t in tables if t.pg_rows == -1 or t.mysql_rows == -1]
        else:  # all
            return tables
    
    def _sort_tables(self, tables: List[TableInfo]) -> List[TableInfo]:
        """根据当前排序方式对表格进行排序"""
        if self.sort_by == "data_diff":
            # 按数据差异排序，差异大的在前
            return sorted(tables, key=lambda t: abs(t.data_diff) if t.data_diff != 0 else -1, reverse=True)
        elif self.sort_by == "pg_rows":
            # 按PG记录数排序，多的在前
            return sorted(tables, key=lambda t: t.pg_rows if t.pg_rows != -1 else -1, reverse=True)
        elif self.sort_by == "mysql_rows":
            # 按MySQL记录数排序，多的在前
            return sorted(tables, key=lambda t: t.mysql_rows if t.mysql_rows != -1 else -1, reverse=True)
        else:  # schema_table
            # 按schema名和表名排序
            return sorted(tables, key=lambda t: (t.schema_name, t.target_table_name))
    
    def _update_data_table(self):
        """更新数据表格"""
        table = self.query_one("#tables", DataTable)
        
        # 先过滤再排序
        filtered_tables = self._filter_tables(self.tables)
        sorted_tables = self._sort_tables(filtered_tables)
        
        # 保存当前光标位置和滚动位置
        current_cursor = table.cursor_coordinate if table.row_count > 0 else None
        current_scroll_y = table.scroll_y if hasattr(table, 'scroll_y') else 0
        
        # 清空表格并重新填充
        table.clear()
        
        for i, t in enumerate(sorted_tables, 1):
            # 状态图标
            if t.pg_rows == -1 or t.mysql_rows == -1:
                icon = "❌"
            elif t.is_consistent:
                icon = "✅"
            else:
                icon = "⚠️"
                
            # 数据差异文本和样式 - 零值与变化量保持一致
            if t.pg_rows == -1 or t.mysql_rows == -1:
                diff_text = "[bold bright_red]ERROR[/]"  # 错误状态用亮红色
            else:
                # 根据差异大小和方向使用不同颜色
                if t.data_diff < 0:
                    diff_text = f"[bold orange3]{t.data_diff:+,}[/]"  # 负数用橙色（PG落后）
                elif t.data_diff > 0:
                    diff_text = f"[bold bright_green]{t.data_diff:+,}[/]"  # 正数用亮绿色（PG领先）
                else:
                    diff_text = "[dim white]0[/]"  # 零用暗白色（与变化量一致）
                
            # 变化量文本和样式 - 去掉无变化时的横线
            if t.pg_rows == -1:
                change_text = "[bold bright_red]ERROR[/]"
            elif t.change > 0:
                change_text = f"[bold spring_green3]+{t.change:,} ⬆[/]"  # 增加用春绿色
            elif t.change < 0:
                change_text = f"[bold orange3]{t.change:,} ⬇[/]"  # 减少用橙色
            else:
                change_text = "[dim white]0[/]"  # 无变化只显示0，与数据差异保持一致
                
            # MySQL更新时间和样式 - 与PG更新时间保持一致
            if t.mysql_updating:
                mysql_status = "[yellow3]更新中[/]"  # 使用更温和的深黄色
            else:
                mysql_relative_time = self.get_relative_time(t.mysql_last_updated)
                if "年前" in mysql_relative_time or "个月前" in mysql_relative_time:
                    mysql_status = f"[bold orange1]{mysql_relative_time}[/]"  # 很久没更新用橙色
                elif "天前" in mysql_relative_time:
                    mysql_status = f"[bold yellow3]{mysql_relative_time}[/]"  # 几天前用深黄色
                elif "小时前" in mysql_relative_time:
                    mysql_status = f"[bright_cyan]{mysql_relative_time}[/]"  # 几小时前用亮青色
                else:
                    mysql_status = f"[dim bright_black]{mysql_relative_time}[/]"  # 最近更新用暗色（与PG一致）
                
            # 记录数显示和样式 - 区分估计值和精确值
            if t.pg_rows == -1:
                pg_rows_display = "[bold bright_red]ERROR[/]"
            elif t.pg_is_estimated:
                pg_rows_display = f"[italic bright_blue]~{t.pg_rows:,}[/]"  # 估计值用斜体亮蓝色
            else:
                pg_rows_display = f"[bold bright_cyan]{t.pg_rows:,}[/]"  # 精确值用亮青色粗体
                
            if t.mysql_rows == -1:
                mysql_rows_display = "[bold bright_red]ERROR[/]"
            elif t.mysql_is_estimated:
                mysql_rows_display = f"[italic medium_purple1]~{t.mysql_rows:,}[/]"  # 估计值用斜体中紫色
            else:
                mysql_rows_display = f"[bold bright_magenta]{t.mysql_rows:,}[/]"  # 精确值用亮洋红色粗体
            
            # Schema名称和表名样式 - 使用更清晰的颜色
            schema_display = f"[bold medium_purple3]{t.schema_name[:12] + '...' if len(t.schema_name) > 15 else t.schema_name}[/]"  # Schema用中紫色
            table_display = f"[bold dodger_blue2]{t.target_table_name[:35] + '...' if len(t.target_table_name) > 38 else t.target_table_name}[/]"  # 表名用道奇蓝色
            
            # PG更新时间样式 - 区分更新状态，使用更温和的颜色
            if t.pg_updating:
                pg_time_display = "[yellow3]更新中[/]"  # 使用更温和的深黄色
            else:
                pg_relative_time = self.get_relative_time(t.last_updated)
                if "年前" in pg_relative_time or "个月前" in pg_relative_time:
                    pg_time_display = f"[bold orange1]{pg_relative_time}[/]"  # 很久没更新用橙色
                elif "天前" in pg_relative_time:
                    pg_time_display = f"[bold yellow3]{pg_relative_time}[/]"  # 几天前用深黄色
                elif "小时前" in pg_relative_time:
                    pg_time_display = f"[bright_cyan]{pg_relative_time}[/]"  # 几小时前用亮青色
                else:
                    pg_time_display = f"[dim bright_black]{pg_relative_time}[/]"  # 最近更新用暗色
            
            # 源表数量样式 - 使用原来MySQL更新时间的颜色方案
            source_count = len(t.mysql_source_tables)
            if source_count >= 5:
                source_count_display = f"[bold orange1]{source_count}[/]"  # 源表多用橙色
            elif source_count >= 3:
                source_count_display = f"[bold yellow3]{source_count}[/]"  # 中等数量用深黄色
            elif source_count >= 2:
                source_count_display = f"[bright_cyan]{source_count}[/]"  # 少量用亮青色
            else:
                source_count_display = f"[dim bright_white]{source_count}[/]"  # 单表用暗亮白色
            
            # 添加行到表格
            table.add_row(
                str(i),
                icon,
                schema_display,
                table_display,
                pg_rows_display,
                mysql_rows_display,
                diff_text,
                change_text,
                pg_time_display,
                mysql_status,
                source_count_display
            )
        
        # 尝试恢复光标位置和滚动位置
        if current_cursor is not None and table.row_count > 0:
            try:
                # 恢复光标位置
                new_row = min(current_cursor.row, table.row_count - 1)
                table.move_cursor(row=new_row)
                
                # 多种方式尝试恢复滚动位置
                self.call_after_refresh(self._restore_scroll_position, table, current_scroll_y)
                    
            except Exception:
                pass  # 如果恢复失败，保持默认位置
    
    def _restore_scroll_position(self, table: DataTable, scroll_y: int):
        """恢复滚动位置的辅助方法"""
        try:
            # 尝试多种方式恢复滚动位置
            if hasattr(table, 'scroll_y'):
                table.scroll_y = scroll_y
            if hasattr(table, 'scroll_to'):
                table.scroll_to(y=scroll_y, animate=False)
            if hasattr(table, 'scroll_offset'):
                table.scroll_offset = table.scroll_offset.replace(y=scroll_y)
        except Exception:
            pass  # 静默失败，不影响正常功能
    
    async def refresh_data(self):
        """定时刷新数据"""
        if self.stop_event.is_set() or self.is_paused:
            return
            
        # 重新构建target_tables结构用于更新
        target_tables = {}
        for table_info in self.tables:
            schema_name = table_info.schema_name
            if schema_name not in target_tables:
                target_tables[schema_name] = {}
            target_tables[schema_name][table_info.target_table_name] = table_info
            
        # 更新PostgreSQL记录数
        self.pg_iteration += 1
        await self.update_postgresql_counts_async(target_tables)
        
        # 按间隔更新MySQL记录数
        if self.pg_iteration % self.mysql_update_interval == 0:
            self.mysql_iteration += 1
            await self.update_mysql_counts_async(target_tables, use_information_schema=False)
            
        # 更新进度跟踪数据
        self.update_progress_data(self.tables)
            
        # 更新显示
        self.update_display()
        
    def action_quit(self) -> None:
        """退出应用"""
        self.stop_event.set()
        if self.refresh_timer:
            self.refresh_timer.stop()
        self.exit()
        
    def action_refresh(self) -> None:
        """手动刷新"""
        self.call_later(self.refresh_data)
        
    def action_toggle_pause(self) -> None:
        """暂停/继续监控"""
        self.is_paused = not self.is_paused
        self.update_display()
                
    def action_sort_toggle(self) -> None:
        """切换排序方式"""
        sort_options = ["schema_table", "data_diff", "pg_rows", "mysql_rows"]
        current_index = sort_options.index(self.sort_by)
        self.sort_by = sort_options[(current_index + 1) % len(sort_options)]
        self.update_display()
        
    def action_filter_toggle(self) -> None:
        """切换过滤方式"""
        filter_options = ["all", "inconsistent", "consistent", "error"]
        current_index = filter_options.index(self.filter_mode)
        self.filter_mode = filter_options[(current_index + 1) % len(filter_options)]
        self.update_display()
        
    def _signal_handler(self, signum, frame):
        """信号处理器"""
        self.stop_event.set()
        self.exit()

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

    def update_progress_data(self, tables: List[TableInfo]):
        """更新进度数据，计算总数和变化量"""
        current_time = datetime.now()

        # 过滤掉错误状态的表进行统计
        valid_tables = [t for t in tables if t.pg_rows != -1 and t.mysql_rows != -1]

        total_pg_rows = sum(t.pg_rows for t in valid_tables)
        total_mysql_rows = sum(t.mysql_rows for t in valid_tables)
        total_pg_change = sum(t.change for t in valid_tables)

        # 添加到历史数据
        self.history_data.append((current_time, total_pg_rows, total_mysql_rows, total_pg_change))

        # 保持历史数据在指定范围内
        if len(self.history_data) > self.max_history_points:
            self.history_data.pop(0)

    def calculate_sync_speed(self) -> float:
        """计算同步速度（记录/秒）"""
        if len(self.history_data) < 2:
            return 0.0

        # 使用最近的数据点计算速度
        recent_data = self.history_data[-min(10, len(self.history_data)):]

        if len(recent_data) < 2:
            return 0.0

        # 计算时间跨度和总变化量
        time_span = (recent_data[-1][0] - recent_data[0][0]).total_seconds()
        if time_span <= 0:
            return 0.0

        # 计算PostgreSQL总变化量（所有数据点的变化量之和）
        total_change = sum(data[3] for data in recent_data if data[3] > 0)  # 只计算正向变化

        return total_change / time_span if time_span > 0 else 0.0

    def estimate_remaining_time(self, mysql_total: int, pg_total: int, speed: float) -> str:
        """估算剩余时间"""
        if speed <= 0 or mysql_total <= 0:
            return "无法估算"

        # 计算还需要同步的记录数
        remaining_records = mysql_total - pg_total
        if remaining_records <= 0:
            return "已完成"

        remaining_seconds = remaining_records / speed

        if remaining_seconds < 60:
            return f"{int(remaining_seconds)}秒"
        elif remaining_seconds < 3600:
            minutes = int(remaining_seconds // 60)
            seconds = int(remaining_seconds % 60)
            return f"{minutes}分{seconds}秒"
        elif remaining_seconds < 86400:
            hours = int(remaining_seconds // 3600)
            minutes = int((remaining_seconds % 3600) // 60)
            return f"{hours}小时{minutes}分钟"
        else:
            days = int(remaining_seconds // 86400)
            hours = int((remaining_seconds % 86400) // 3600)
            return f"{days}天{hours}小时"

    async def load_config(self) -> bool:
        """加载配置文件"""
        config_path = Path(self.config_file)
        if not config_path.exists():
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
            if self.override_databases:
                databases_list = self.override_databases
            else:
                databases_list = mysql_section['databases'].split(',')

            self.mysql_config = MySQLConfig(
                host=mysql_section['host'],
                port=int(mysql_section['port']),
                database="",
                username=mysql_section['username'],
                password=mysql_section['password'],
                databases=databases_list,
                ignored_prefixes=mysql_section.get('ignored_table_prefixes', '').split(',')
            )

            # 监控配置
            monitor_section = config['monitor']
            self.monitor_config = {
                'refresh_interval': int(monitor_section.get('refresh_interval', 3)),
                'mysql_update_interval': int(monitor_section.get('mysql_update_interval', 3)),
            }

            self.mysql_update_interval = self.monitor_config['mysql_update_interval']
            return True

        except Exception as e:
            return False

    async def connect_postgresql(self):
        """连接PostgreSQL"""
        try:
            conn = await asyncpg.connect(
                host=self.pg_config.host,
                port=self.pg_config.port,
                database=self.pg_config.database,
                user=self.pg_config.username,
                password=self.pg_config.password,
                timeout=10
            )
            return conn
        except Exception as e:
            return None

    async def connect_mysql(self, database: str):
        """连接MySQL"""
        try:
            conn = await aiomysql.connect(
                host=self.mysql_config.host,
                port=self.mysql_config.port,
                db=database,
                user=self.mysql_config.username,
                password=self.mysql_config.password,
                connect_timeout=5,
                charset='utf8mb4'
            )
            return conn
        except Exception as e:
            return None

    async def initialize_tables_from_mysql(self):
        """从MySQL初始化表结构"""
        schema_tables = {}
        
        for schema_name in self.mysql_config.databases:
            schema_name = schema_name.strip()
            if not schema_name:
                continue

            mysql_conn = await self.connect_mysql(schema_name)
            if not mysql_conn:
                continue

            try:
                async with mysql_conn.cursor() as cursor:
                    await cursor.execute("""
                        SELECT TABLE_NAME
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = %s
                          AND TABLE_TYPE = 'BASE TABLE'
                    """, (schema_name,))

                    mysql_table_names = []
                    rows = await cursor.fetchall()
                    for row in rows:
                        table_name = row[0]
                        if not any(table_name.startswith(prefix.strip())
                                 for prefix in self.mysql_config.ignored_prefixes if prefix.strip()):
                            mysql_table_names.append(table_name)

                # 按目标表名分组
                target_tables = {}
                for mysql_table_name in mysql_table_names:
                    target_table_name = self.sync_props.get_target_table_name(mysql_table_name)

                    if target_table_name not in target_tables:
                        current_time = datetime.now()
                        target_tables[target_table_name] = TableInfo(
                            schema_name=schema_name,
                            target_table_name=target_table_name,
                            mysql_rows=0,
                            mysql_last_updated=current_time - timedelta(days=365),
                            last_updated=current_time
                        )
                        target_tables[target_table_name].mysql_source_tables.append(mysql_table_name)
                    else:
                        target_tables[target_table_name].mysql_source_tables.append(mysql_table_name)

                if target_tables:
                    schema_tables[schema_name] = target_tables

            finally:
                mysql_conn.close()

        return schema_tables



    async def _update_single_schema_mysql(self, schema_name: str, tables_dict: Dict[str, TableInfo],
                                          use_information_schema: bool = False) -> bool:
        """更新单个schema的MySQL记录数（异步版本，支持中断）"""
        current_time = datetime.now()

        # 检查是否收到停止信号
        if self.stop_event.is_set():
            return False

        try:
            mysql_conn = await self.connect_mysql(schema_name)
            if not mysql_conn:
                return False

            try:
                if use_information_schema:
                    # 检查停止标志
                    if self.stop_event.is_set():
                        return False

                    # 第一次运行使用information_schema快速获取估计值
                    async with mysql_conn.cursor() as cursor:
                        await cursor.execute("""
                                             SELECT table_name, table_rows
                                             FROM information_schema.tables
                                             WHERE table_schema = %s
                                               AND table_type = 'BASE TABLE'
                                             ORDER BY table_rows DESC
                                             """, (schema_name,))

                        # 建立表名到行数的映射
                        table_rows_map = {}
                        rows = await cursor.fetchall()
                        for row in rows:
                            table_name, table_rows = row
                            table_rows_map[table_name] = table_rows or 0  # 处理NULL值

                    # 更新TableInfo中的MySQL行数
                    for table_info in tables_dict.values():
                        # 检查停止标志
                        if self.stop_event.is_set():
                            return False

                        async with self.mysql_update_lock:
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
                    # 常规更新使用精确的COUNT查询 - 优化显示逻辑
                    # 首先标记所有表为更新中状态
                    async with self.mysql_update_lock:
                        for table_info in tables_dict.values():
                            if not table_info.mysql_updating:
                                table_info.mysql_updating = True
                    
                    # 然后逐个处理表
                    for table_info in tables_dict.values():
                        # 检查停止标志
                        if self.stop_event.is_set():
                            # 恢复所有表的状态
                            async with self.mysql_update_lock:
                                for ti in tables_dict.values():
                                    ti.mysql_updating = False
                            return False

                        # 在锁外执行查询以避免长时间锁定
                        temp_mysql_rows = 0

                        # 更新所有源表的记录数
                        for mysql_table_name in table_info.mysql_source_tables:
                            # 检查停止标志
                            if self.stop_event.is_set():
                                async with self.mysql_update_lock:
                                    for ti in tables_dict.values():
                                        ti.mysql_updating = False
                                return False

                            try:
                                async with mysql_conn.cursor() as cursor:
                                    # 先尝试使用主键索引进行count查询
                                    try:
                                        await cursor.execute(
                                            f"SELECT COUNT(*) FROM `{mysql_table_name}` USE INDEX (PRIMARY)")
                                        result = await cursor.fetchone()
                                        mysql_rows = result[0]
                                    except Exception:
                                        # 如果使用索引失败（可能没有主键索引），使用普通查询
                                        await cursor.execute(f"SELECT COUNT(*) FROM `{mysql_table_name}`")
                                        result = await cursor.fetchone()
                                        mysql_rows = result[0]
                                temp_mysql_rows += mysql_rows
                            except Exception as e:
                                # 表可能不存在或无权限，跳过
                                continue

                        # 查询完成后更新结果
                        async with self.mysql_update_lock:
                            table_info.mysql_rows = temp_mysql_rows
                            table_info.mysql_last_updated = current_time
                            table_info.mysql_updating = False
                            table_info.mysql_is_estimated = False  # 标记为精确值

                return True
            finally:
                mysql_conn.close()

        except Exception as e:
            # 出现异常时，标记所有表的mysql_updating为False
            async with self.mysql_update_lock:
                for table_info in tables_dict.values():
                    if table_info.mysql_updating:
                        table_info.mysql_updating = False
            return False

    async def update_mysql_counts_async(self, target_tables: Dict[str, Dict[str, TableInfo]],
                                        use_information_schema: bool = False):
        """异步更新MySQL记录数（不阻塞主线程）"""
        # 清理已完成的任务
        self.mysql_update_tasks = [f for f in self.mysql_update_tasks if not f.done()]

        # 为每个schema提交异步更新任务
        for schema_name, tables_dict in target_tables.items():
            # 检查该schema是否已经有正在进行的更新任务
            schema_updating = False
            async with self.mysql_update_lock:
                for table_info in tables_dict.values():
                    if table_info.mysql_updating:
                        schema_updating = True
                        break

            if not schema_updating:
                future = asyncio.create_task(
                    self._update_single_schema_mysql(schema_name, tables_dict, use_information_schema))
                self.mysql_update_tasks.append(future)

    async def update_mysql_counts(self, target_tables: Dict[str, Dict[str, TableInfo]],
                                  use_information_schema: bool = False):
        """更新MySQL记录数（同步版本，用于兼容性）"""
        for schema_name, tables_dict in target_tables.items():
            await self._update_single_schema_mysql(schema_name, tables_dict, use_information_schema)

    async def get_postgresql_rows_from_pg_stat(self, conn, target_tables: Dict[str, Dict[str, TableInfo]]):
        """第一次运行时使用pg_stat_user_tables快速获取PostgreSQL表行数估计值"""
        current_time = datetime.now()
        self.pg_updating = True

        try:
            for schema_name, tables_dict in target_tables.items():
                try:
                    # 一次性获取该schema下所有表的统计信息
                    rows = await conn.fetch("""
                                            SELECT relname, n_tup_ins - n_tup_del + n_tup_upd AS estimated_rows
                                            FROM pg_stat_user_tables
                                            WHERE schemaname = $1
                                            """, schema_name)

                    # 建立表名到估计行数的映射
                    pg_stats_map = {}
                    for row in rows:
                        table_name, estimated_rows = row['relname'], row['estimated_rows']
                        pg_stats_map[table_name] = max(0, estimated_rows or 0)  # 确保非负数

                    # 更新TableInfo
                    for target_table_name, table_info in tables_dict.items():
                        if target_table_name in pg_stats_map:
                            new_count = pg_stats_map[target_table_name]
                        else:
                            # 如果统计信息中没有，可能是新表或无数据，使用精确查询
                            try:
                                result = await conn.fetchval(
                                    f'SELECT COUNT(*) FROM "{schema_name}"."{target_table_name}"')
                                new_count = result
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
                    await self.update_postgresql_counts(conn, {schema_name: tables_dict})
        finally:
            self.pg_updating = False

    async def _update_single_schema_postgresql(self, schema_name: str, tables_dict: Dict[str, TableInfo]) -> bool:
        """更新单个schema的PostgreSQL记录数（异步版本，支持中断）"""
        current_time = datetime.now()
        
        # 检查是否收到停止信号
        if self.stop_event.is_set():
            return False
            
        try:
            conn = await self.connect_postgresql()
            if not conn:
                return False

            try:
                # 常规更新使用精确的COUNT查询 - 优化显示逻辑  
                # 首先标记所有表为更新中状态
                async with self.pg_update_lock:
                    for table_info in tables_dict.values():
                        if not table_info.pg_updating:
                            table_info.pg_updating = True
                
                # 然后逐个处理表
                for target_table_name, table_info in tables_dict.items():
                    # 检查停止标志
                    if self.stop_event.is_set():
                        # 恢复所有表的状态
                        async with self.pg_update_lock:
                            for ti in tables_dict.values():
                                ti.pg_updating = False
                        return False

                    # 在锁外执行查询以避免长时间锁定
                    try:
                        # 直接获取记录数
                        new_count = await conn.fetchval(f'SELECT COUNT(*) FROM "{schema_name}"."{target_table_name}"')

                        # 查询完成后更新结果
                        async with self.pg_update_lock:
                            if not table_info.is_first_query:
                                table_info.previous_pg_rows = table_info.pg_rows
                            else:
                                table_info.previous_pg_rows = new_count
                                table_info.is_first_query = False

                            table_info.pg_rows = new_count
                            table_info.last_updated = current_time
                            table_info.pg_updating = False
                            table_info.pg_is_estimated = False  # 标记为精确值

                    except Exception as e:
                        # 出现异常时标记为错误状态
                        async with self.pg_update_lock:
                            if not table_info.is_first_query:
                                table_info.previous_pg_rows = table_info.pg_rows
                            else:
                                table_info.previous_pg_rows = -1
                                table_info.is_first_query = False

                            table_info.pg_rows = -1  # -1表示查询失败
                            table_info.last_updated = current_time
                            table_info.pg_updating = False
                            table_info.pg_is_estimated = False  # 错误状态不是估计值

                return True
            finally:
                await conn.close()

        except Exception as e:
            # 出现异常时，标记所有表的pg_updating为False
            async with self.pg_update_lock:
                for table_info in tables_dict.values():
                    if table_info.pg_updating:
                        table_info.pg_updating = False
            return False

    async def update_postgresql_counts_async(self, target_tables: Dict[str, Dict[str, TableInfo]]):
        """异步更新PostgreSQL记录数（不阻塞主线程）"""
        # 清理已完成的任务
        self.pg_update_tasks = [f for f in self.pg_update_tasks if not f.done()]
        
        # 检查是否已经有正在进行的更新任务
        if self.pg_updating:
            return
            
        # 为每个schema提交异步更新任务
        for schema_name, tables_dict in target_tables.items():
            # 检查该schema是否已经有正在进行的更新任务
            schema_updating = False
            async with self.pg_update_lock:
                for table_info in tables_dict.values():
                    if table_info.pg_updating:
                        schema_updating = True
                        break
            
            if not schema_updating:
                future = asyncio.create_task(self._update_single_schema_postgresql(schema_name, tables_dict))
                self.pg_update_tasks.append(future)

    async def update_postgresql_counts(self, conn, target_tables: Dict[str, Dict[str, TableInfo]]):
        """更新PostgreSQL记录数（同步版本，用于兼容性）"""
        current_time = datetime.now()
        self.pg_updating = True
        try:
            await self._update_postgresql_counts_exact(conn, target_tables, current_time)
        finally:
            self.pg_updating = False

    async def _update_postgresql_counts_exact(self, conn, target_tables: Dict[str, Dict[str, TableInfo]], current_time):
        """使用精确COUNT查询更新PostgreSQL记录数"""
        for schema_name, tables_dict in target_tables.items():
            for target_table_name, table_info in tables_dict.items():
                try:
                    # 直接获取记录数
                    new_count = await conn.fetchval(f'SELECT COUNT(*) FROM "{schema_name}"."{target_table_name}"')

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





def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="PostgreSQL数据库监控工具 (Textual版本)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  python3 cdc_monitor.py                          # 使用配置文件中的数据库列表
  python3 cdc_monitor.py --databases db1,db2     # 监控指定的数据库
  python3 cdc_monitor.py -d test_db               # 只监控test_db数据库
  python3 cdc_monitor.py --config my_config.ini  # 使用指定的配置文件

快捷键:
  q/Ctrl+C : 退出程序
  r        : 手动刷新数据
  space    : 暂停/继续监控
  s        : 切换排序方式 (Schema.表名 → 数据差异 → PG记录数 → MySQL记录数)
  f        : 切换过滤方式 (全部 → 不一致 → 一致 → 错误)
  方向键   : 移动光标浏览表格
  Page Up/Down : 快速翻页
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
        override_databases = [db.strip() for db in args.databases.split(',') if db.strip()]
        if not override_databases:
            print("❌ 指定的数据库列表为空")
            sys.exit(1)

    app = MonitorApp(config_file, override_databases)
    app.run()


if __name__ == "__main__":
    main()
