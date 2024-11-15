cmake_minimum_required(VERSION 3.10)
project(leveldb_examples)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 设置 LevelDB 的路径
set(LEVELDB_BUILD_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../build)
set(LEVELDB_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../include)

# 查找 Snappy 库
find_package(Snappy QUIET)
if(NOT Snappy_FOUND)
    message(STATUS "System Snappy not found. Please install snappy-dev package.")
endif()

# 添加可执行文件
add_executable(basic_example basic_example.cpp)

# 包含 LevelDB 头文件
target_include_directories(basic_example PRIVATE ${LEVELDB_INCLUDE_DIR})

# 链接 LevelDB 库和 Snappy
target_link_libraries(basic_example PRIVATE 
    ${LEVELDB_BUILD_DIR}/libleveldb.a
    snappy
    pthread
) 