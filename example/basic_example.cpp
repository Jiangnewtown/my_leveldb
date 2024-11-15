#include <iostream>
#include <fstream>
#include <sstream>
#include <cassert>
#include <string>
#include <thread>
#include <chrono>
#include <map>
#include <vector>
#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/cache.h"
#include "nlohmann/json.hpp"

using json = nlohmann::json;

// TMDB电影数据结构
struct Movie {
    std::string id;
    std::map<std::string, std::string> fields;  // 存储所有字段
    
    // 序列化为JSON字符串
    std::string serialize() const {
        json j = fields;
        return j.dump();
    }
    
    // 从JSON字符串反序列化
    static Movie deserialize(const std::string& id, const std::string& value) {
        Movie movie;
        movie.id = id;
        movie.fields = json::parse(value).get<std::map<std::string, std::string>>();
        return movie;
    }
};

// 从TMDB CSV文件加载电影数据
std::vector<Movie> loadMoviesFromCSV(const std::string& filename) {
    std::vector<Movie> movies;
    std::ifstream file(filename);
    
    if (!file.is_open()) {
        std::cerr << "无法打开文件: " << filename << std::endl;
        return movies;
    }
    
    std::string line;
    std::vector<std::string> headers;
    
    // 读取标题行
    if (std::getline(file, line)) {
        std::istringstream ss(line);
        std::string field;
        // 解析标题行（逗号分隔）
        while (std::getline(ss, field, ',')) {
            headers.push_back(field);
        }
        std::cout << "读取到 " << headers.size() << " 个字段" << std::endl;
    }
    
    // 读取数据行
    int line_number = 1;
    while (std::getline(file, line)) {
        line_number++;
        if (line.empty()) continue;
        
        try {
            std::vector<std::string> fields;
            size_t start = 0;
            size_t end = 0;
            bool in_quotes = false;
            std::string current_field;
            
            // 手动解析CSV，处理引号内的逗号
            for (size_t i = 0; i < line.length(); i++) {
                char c = line[i];
                if (c == '"') {
                    in_quotes = !in_quotes;
                } else if (c == ',' && !in_quotes) {
                    fields.push_back(current_field);
                    current_field.clear();
                } else {
                    current_field += c;
                }
            }
            fields.push_back(current_field); // 添加最后一个字段
            
            if (fields.size() == headers.size()) {
                Movie movie;
                movie.id = fields[0];  // 第一个字段是id
                
                // 将所有字段添加到map中
                for (size_t i = 0; i < headers.size(); i++) {
                    movie.fields[headers[i]] = fields[i];
                }
                
                movies.push_back(movie);
                
                if (movies.size() % 1000 == 0) {
                    std::cout << "已加载 " << movies.size() << " 部电影..." << std::endl;
                }
            } else {
                std::cerr << "第 " << line_number << " 行字段数不匹配 (期望 " 
                         << headers.size() << ", 实际 " << fields.size() << ")" << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "处理第 " << line_number << " 行时出错: " << e.what() << std::endl;
        }
    }
    
    std::cout << "文件读取完成，共加载 " << movies.size() << " 部电影" << std::endl;
    
    // 显示前几条数据作为样本
    if (!movies.empty()) {
        std::cout << "\n数据样本（前3条）：" << std::endl;
        for (int i = 0; i < std::min(3, static_cast<int>(movies.size())); i++) {
            const auto& movie = movies[i];
            std::cout << "电影 " << i + 1 << ":\n"
                     << "ID: " << movie.id << "\n"
                     << "JSON: " << movie.serialize() << "\n"
                     << std::endl;
        }
    }
    
    return movies;
}

// 打印当前数据库状态的辅助函数
void printDBStats(leveldb::DB* db) {
    std::string stats;
    
    std::cout << "\n=== 当前数据库状态 ===" << std::endl;
    
    if (db->GetProperty("leveldb.stats", &stats)) {
        // 解析并格式化统计信息
        std::istringstream ss(stats);
        std::string line;
        while (std::getline(ss, line)) {
            if (line.find("Compactions") != std::string::npos) {
                // 打印标题行
                std::cout << "Compactions" << std::endl;
                std::cout << "Level    Files  Size(MB)  Time(sec)  Read(MB)  Write(MB)" << std::endl;
                std::cout << "--------------------------------------------------------" << std::endl;
            } else if (!line.empty() && line[0] >= '0' && line[0] <= '9') {
                // 解析数据行
                std::istringstream line_ss(line);
                int level, files;
                double size, time, read, write;
                line_ss >> level >> files >> size >> time >> read >> write;
                
                // 格式化输出
                printf("  %-6d  %-6d  %-8.1f  %-9.1f  %-8.1f  %-8.1f\n",
                       level, files, size, time, read, write);
            }
        }
    }
    
    // 打印每个level的文件数
    std::cout << "\n文件分布：" << std::endl;
    std::cout << "Level    Files" << std::endl;
    std::cout << "-------------" << std::endl;
    for (int level = 0; level < 7; level++) {
        std::string prop = "leveldb.num-files-at-level" + std::to_string(level);
        if (db->GetProperty(prop, &stats)) {
            printf("  %-6d  %-6s\n", level, stats.c_str());
        }
    }
    
    // 打印SST文件详情
    if (db->GetProperty("leveldb.sstables", &stats)) {
        std::cout << "\nSST文件详情：" << std::endl;
        std::cout << stats << std::endl;
    }
}

int main() {
    // 配置数据库选项
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 4 << 10;    // 改为4KB，更容易触发刷盘
    options.max_file_size = 2 << 10;        // 改为2KB，更容易触发文件分裂
    options.filter_policy = leveldb::NewBloomFilterPolicy(10);
    options.block_cache = leveldb::NewLRUCache(8 << 20); // 8MB cache

    // 打开数据库
    std::cout << "=== 开始打开数据库 ===" << std::endl;
    leveldb::Status status = leveldb::DB::Open(options, "./tmdb_moviedb", &db);
    if (!status.ok()) {
        std::cerr << "无法打开/创建数据库: " << status.ToString() << std::endl;
        return 1;
    }

    // 加载电影数据
    std::cout << "\n=== 加载TMDB电影数据 ===" << std::endl;
    auto movies = loadMoviesFromCSV("tmdb-movies.csv");
    std::cout << "加载了 " << movies.size() << " 部电影" << std::endl;

    // 批量写入数据
    std::cout << "\n=== 开始写入数据 ===" << std::endl;
    {
        leveldb::WriteBatch batch;
        int count = 0;
        
        for (const auto& movie : movies) {
            std::string value = movie.serialize();
            std::cout << "写入电影 ID: " << movie.id << ", 数据大小: " << value.size() << " 字节" << std::endl;
            
            batch.Put(movie.id, value);
            count++;
            
            // 每10条数据写入一次，更频繁地触发刷盘
            if (count % 10 == 0) {
                status = db->Write(leveldb::WriteOptions(), &batch);
                assert(status.ok());
                batch.Clear();
                
                std::cout << "\n写入 " << count << " 部电影后：" << std::endl;
                printDBStats(db);
                
                // 每写入100条数据就触发一次手动压缩
                if (count % 100 == 0) {
                    std::cout << "\n触发手动压缩..." << std::endl;
                    db->CompactRange(nullptr, nullptr);
                    std::cout << "压缩后状态：" << std::endl;
                    printDBStats(db);
                }
            }
        }
        
        // 写入剩余数据
        if (status.ok()) {
            status = db->Write(leveldb::WriteOptions(), &batch);
            assert(status.ok());
            std::cout << "\n最终写入完成，触发最后一次压缩..." << std::endl;
            db->CompactRange(nullptr, nullptr);
            printDBStats(db);
        }
    }

    // 验证数据
    std::cout << "\n=== 验证数据 ===" << std::endl;
    std::string value;
    for (int i = 0; i < std::min(5, static_cast<int>(movies.size())); i++) {
        status = db->Get(leveldb::ReadOptions(), movies[i].id, &value);
        if (status.ok()) {
            std::cout << "ID " << movies[i].id << " 的数据：\n" << value << std::endl;
        }
    }

    // 清理资源
    delete db;
    delete options.filter_policy;
    delete options.block_cache;

    std::cout << "\n示例运行完成！" << std::endl;
    return 0;
} 