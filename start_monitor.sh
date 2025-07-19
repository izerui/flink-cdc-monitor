#!/bin/bash

# PostgreSQL数据库监控工具 - Textual版本启动脚本

echo "启动 PostgreSQL 数据库监控工具 (Textual版本)..."
echo "================================================="

# 激活虚拟环境（如果存在）
if [ -d ".venv" ]; then
    echo "激活虚拟环境..."
    source .venv/bin/activate
elif [ -d "venv" ]; then
    echo "激活虚拟环境..."
    source venv/bin/activate
fi

# 检查Python版本
python_version=$(python3 --version 2>&1)
echo "Python版本: $python_version"

# 检查依赖
echo "检查依赖..."
python3 -c "import textual; print('✅ textual 已安装')" 2>/dev/null || echo "❌ textual 未安装，请运行: pip install textual"
python3 -c "import rich; print('✅ rich 已安装')" 2>/dev/null || echo "❌ rich 未安装，请运行: pip install rich"
python3 -c "import asyncpg; print('✅ asyncpg 已安装')" 2>/dev/null || echo "❌ asyncpg 未安装，请运行: pip install asyncpg"
python3 -c "import aiomysql; print('✅ aiomysql 已安装')" 2>/dev/null || echo "❌ aiomysql 未安装，请运行: pip install aiomysql"

echo ""
echo "🚀 启动Textual版监控程序..."
echo "📋 新功能特性："
echo "   ✨ 使用滚动表格，无需分页翻转"
echo "   🎯 支持智能过滤和多种排序"
echo "   ⚡ 更流畅的用户体验"
echo ""
echo "⌨️  快捷键操作："
echo "   q/Ctrl+C : 退出程序"
echo "   r        : 手动刷新数据"
echo "   space    : 暂停/继续监控"
echo "   s        : 切换排序方式"
echo "   f        : 切换过滤方式"
echo "   方向键   : 移动光标浏览表格"
echo "   Page Up/Down : 快速翻页"
echo "   Home/End : 跳转到顶部/底部"
echo ""

# 检查配置文件
if [ ! -f "config.ini" ]; then
    echo "❌ 配置文件 config.ini 不存在"
    echo "请复制 config.ini.example 并修改配置"
    exit 1
fi

# 运行监控程序
python3 cdc_monitor.py "$@"

echo ""
echo "监控程序已退出"
