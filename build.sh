#!/bin/bash

# 确保脚本在出错时停止执行
set -e

echo "配置和更新 git submodules..."

# 重置子模块
echo "重置子模块..."
git submodule deinit -f .

# 同步子模块配置
echo "同步子模块配置..."
git submodule sync

# 初始化和更新子模块
echo "初始化和更新子模块..."
git submodule update --init --recursive

echo "开始编译 LevelDB..."

# 创建构建目录
mkdir -p build
cd build

# 运行 CMake 配置
echo "配置 CMake..."
cmake -DCMAKE_BUILD_TYPE=Release ..

# 编译
echo "开始构建..."
cmake --build . -j$(nproc)

echo "编译完成！"

# 显示编译结果位置
echo "编译产物位置："
echo "静态库: $(pwd)/libleveldb.a"
echo "动态库: $(pwd)/libleveldb.so" 