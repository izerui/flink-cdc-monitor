#!/bin/bash

# CDC数据一致性监控一键启动脚本

set -e  # 遇到错误立即退出

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

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
echo -e "${YELLOW}启动监控程序... (按 Ctrl+C 停止)${NC}"
echo ""

# 启动监控程序
uv run cdc_monitor.py 