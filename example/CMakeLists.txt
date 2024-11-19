cmake_minimum_required(VERSION 3.10)
project(leveldb_examples)

# 使用 C++14 标准
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 包含 nlohmann/json，使用 Gitee 镜像
include(FetchContent)
FetchContent_Declare(
    json
    GIT_REPOSITORY https://gitee.com/mirrors/JSON-for-Modern-CPP.git
    GIT_TAG v3.11.2
)
FetchContent_MakeAvailable(json)

# 查找 Snappy 库
find_package(Snappy REQUIRED)

# 设置 LevelDB 的路径
set(LEVELDB_BUILD_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../build)
set(LEVELDB_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../include)

# 添加可执行文件
add_executable(basic_example basic_example.cpp)

# 包含 LevelDB 头文件
target_include_directories(basic_example PRIVATE ${LEVELDB_INCLUDE_DIR})

# 链接 LevelDB 库、JSON 和 Snappy
target_link_libraries(basic_example PRIVATE 
    ${LEVELDB_BUILD_DIR}/libleveldb.a
    nlohmann_json::nlohmann_json
    snappy
    pthread
)

# 添加订单示例可执行文件
add_executable(order_example order_example.cpp)

# 包含 LevelDB 头文件
target_include_directories(order_example PRIVATE ${LEVELDB_INCLUDE_DIR})

# 链接 LevelDB 库和 JSON
target_link_libraries(order_example PRIVATE 
    ${LEVELDB_BUILD_DIR}/libleveldb.a
    nlohmann_json::nlohmann_json
    snappy
    pthread
) 