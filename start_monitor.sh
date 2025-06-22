#!/bin/bash

# CDC数据一致性监控一键启动脚本

set -e  # 遇到错误立即退出

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# 解析命令行参数
DATABASES=""
HELP=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --databases)
            DATABASES="$2"
            shift 2
            ;;
        -d)
            DATABASES="$2" 
            shift 2
            ;;
        --help|-h)
            HELP=true
            shift
            ;;
        *)
            echo -e "${RED}未知参数: $1${NC}"
            HELP=true
            shift
            ;;
    esac
done

# 显示帮助信息
if [ "$HELP" = true ]; then
    echo -e "${BLUE}======================================"
    echo -e "CDC 数据一致性监控 - 使用说明"
    echo -e "======================================${NC}"
    echo ""
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  --databases, -d <数据库列表>   指定要监控的MySQL数据库（逗号分隔）"
    echo "                                覆盖配置文件中的databases配置"
    echo "  --help, -h                    显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0                            # 使用配置文件中的数据库列表"
    echo "  $0 --databases db1,db2,db3   # 监控指定的数据库"
    echo "  $0 -d test_db                # 只监控test_db数据库"
    echo ""
    exit 0
fi

echo -e "${BLUE}======================================"
echo -e "CDC 数据一致性监控 (Python版本)"
echo -e "======================================${NC}"

# 检查uv是否安装
if ! command -v uv &> /dev/null; then
    echo -e "${YELLOW}正在安装uv...${NC}"
    pip install uv || {
        echo -e "${RED}错误: uv安装失败${NC}"
        exit 1
    }
    echo -e "${GREEN}uv安装完成${NC}"
fi

# 安装依赖
echo -e "${BLUE}正在安装依赖...${NC}"
uv sync || {
    echo -e "${RED}错误: 依赖安装失败${NC}"
    exit 1
}

echo -e "${GREEN}依赖安装完成${NC}"

# 显示启动信息
if [ -n "$DATABASES" ]; then
    echo -e "${YELLOW}启动监控程序... (指定数据库: $DATABASES)${NC}"
    echo -e "${YELLOW}(按 Ctrl+C 停止)${NC}"
else
    echo -e "${YELLOW}启动监控程序... (使用配置文件数据库列表)${NC}"
    echo -e "${YELLOW}(按 Ctrl+C 停止)${NC}"
fi
echo ""

# 启动监控程序
if [ -n "$DATABASES" ]; then
    uv run cdc_monitor.py --databases "$DATABASES"
else
    uv run cdc_monitor.py
fi 