#!/bin/bash

# CDC数据一致性监控一键启动脚本 (异步版本)

set -e  # 遇到错误立即退出

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
CYAN='\033[0;36m'
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
    echo -e "${CYAN}新特性 (异步版本):${NC}"
    echo "  • 使用 asyncio 提升性能"
    echo "  • 并发数据库查询，减少等待时间"
    echo "  • 更好的响应性和资源利用率"
    echo ""
    exit 0
fi

echo -e "${BLUE}======================================"
echo -e "CDC 数据一致性监控 (异步版本)"
echo -e "======================================${NC}"

# 检查Python版本
python_version=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
required_version="3.7"

if [ "$(printf '%s\n' "$required_version" "$python_version" | sort -V | head -n1)" != "$required_version" ]; then
    echo -e "${RED}错误: 需要 Python 3.7+ (当前版本: $python_version)${NC}"
    echo -e "${YELLOW}异步版本需要较新的Python版本以支持asyncio特性${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Python版本检查通过: $python_version${NC}"

# 检查uv是否安装
if ! command -v uv &> /dev/null; then
    echo -e "${YELLOW}正在安装uv...${NC}"
    pip install uv || {
        echo -e "${RED}错误: uv安装失败${NC}"
        exit 1
    }
    echo -e "${GREEN}✓ uv安装完成${NC}"
else
    echo -e "${GREEN}✓ uv已安装${NC}"
fi

# 检查异步数据库驱动是否已安装
echo -e "${BLUE}检查异步数据库驱动...${NC}"

check_package() {
    python3 -c "import $1" 2>/dev/null
}

missing_packages=()

if ! check_package "asyncpg"; then
    missing_packages+=("asyncpg")
fi

if ! check_package "aiomysql"; then
    missing_packages+=("aiomysql")
fi

if ! check_package "rich"; then
    missing_packages+=("rich")
fi

# 安装缺失的包
if [ ${#missing_packages[@]} -gt 0 ]; then
    echo -e "${YELLOW}正在安装异步数据库驱动: ${missing_packages[*]}${NC}"
    
    # 优先使用uv安装，如果失败则使用pip
    if ! uv add "${missing_packages[@]}" 2>/dev/null; then
        echo -e "${YELLOW}uv安装失败，尝试使用pip...${NC}"
        pip install "${missing_packages[@]}" || {
            echo -e "${RED}错误: 异步数据库驱动安装失败${NC}"
            echo -e "${YELLOW}请手动安装: pip install asyncpg aiomysql rich${NC}"
            exit 1
        }
    fi
    echo -e "${GREEN}✓ 异步数据库驱动安装完成${NC}"
else
    echo -e "${GREEN}✓ 异步数据库驱动已安装${NC}"
fi

# 安装其他依赖
echo -e "${BLUE}正在安装项目依赖...${NC}"
if [ -f "pyproject.toml" ]; then
    uv sync || {
        echo -e "${YELLOW}uv sync失败，尝试备用方案...${NC}"
        pip install -r requirements.txt 2>/dev/null || {
            echo -e "${YELLOW}未找到requirements.txt，跳过依赖安装${NC}"
        }
    }
    echo -e "${GREEN}✓ 项目依赖安装完成${NC}"
else
    echo -e "${YELLOW}未找到pyproject.toml，跳过项目依赖安装${NC}"
fi

# 检查配置文件
if [ ! -f "config.ini" ]; then
    echo -e "${YELLOW}⚠️  未找到config.ini配置文件${NC}"
    if [ -f "config.ini.example" ]; then
        echo -e "${BLUE}发现示例配置文件，请复制并修改:${NC}"
        echo -e "${CYAN}cp config.ini.example config.ini${NC}"
        echo -e "${CYAN}然后编辑config.ini文件配置数据库连接信息${NC}"
    else
        echo -e "${YELLOW}请创建config.ini配置文件${NC}"
    fi
    echo ""
fi

# 显示启动信息
echo -e "${CYAN}========================================${NC}"
if [ -n "$DATABASES" ]; then
    echo -e "${YELLOW}🚀 启动异步监控程序... (指定数据库: $DATABASES)${NC}"
else
    echo -e "${YELLOW}🚀 启动异步监控程序... (使用配置文件数据库列表)${NC}"
fi
echo -e "${YELLOW}💡 异步版本特性: 并发查询、更快响应、更低资源占用${NC}"
echo -e "${YELLOW}⏹️  按 Ctrl+C 停止监控${NC}"
echo -e "${CYAN}========================================${NC}"
echo ""

# 启动监控程序
if [ -n "$DATABASES" ]; then
    if command -v uv &> /dev/null && [ -f "pyproject.toml" ]; then
        uv run python3 cdc_monitor.py --databases "$DATABASES"
    else
        python3 cdc_monitor.py --databases "$DATABASES"
    fi
else
    if command -v uv &> /dev/null && [ -f "pyproject.toml" ]; then
        uv run python3 cdc_monitor.py
    else
        python3 cdc_monitor.py
    fi
fi 