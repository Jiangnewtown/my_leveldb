#!/bin/bash

# 确保脚本在出错时停止执行
set -e

# 检查 LevelDB 静态库是否存在
LEVELDB_STATIC_LIB="../build/libleveldb.a"
if [ ! -f "$LEVELDB_STATIC_LIB" ]; then
    echo "错误: LevelDB 静态库不存在于 $LEVELDB_STATIC_LIB"
    echo "请先在主目录下运行 build.sh 编译 LevelDB"
    exit 1
fi

echo "检测到 LevelDB 静态库，开始编译示例..."

# 创建构建目录
mkdir -p build
cd build

# 运行 CMake 配置
echo "配置 CMake..."
cmake ..

# 编译
echo "开始构建..."
cmake --build .

echo "编译完成！"
echo "可执行文件位置: $(pwd)/basic_example"
echo ""
echo "运行示例程序："
echo "./build/basic_example" 