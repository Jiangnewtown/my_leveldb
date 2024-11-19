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
    
    std::cout << "\n=== LevelDB 状态分析 ===" << std::endl;
    
    // 1. 分析LSM树的层级结构
    std::cout << "\n1. LSM树层级结构：" << std::endl;
    std::cout << "Level  文件数  说明" << std::endl;
    std::cout << "----------------------------------------" << std::endl;
    int total_files = 0;
    for (int level = 0; level < 7; level++) {
        std::string prop = "leveldb.num-files-at-level" + std::to_string(level);
        if (db->GetProperty(prop, &stats)) {
            int num_files = std::stoi(stats);
            total_files += num_files;
            std::string explanation;
            switch(level) {
                case 0:
                    explanation = "内存表刷盘区域，文件可能有重叠";
                    break;
                case 1:
                    explanation = "第一层压缩，开始排序";
                    break;
                default:
                    explanation = "已完全排序，文件大小约为上层" + std::to_string(1<<level) + "倍";
            }
            printf("L%-5d %-7d %s\n", level, num_files, explanation.c_str());
        }
    }
    std::cout << "总文件数: " << total_files << std::endl;

    // 2. 分析SST文件的详细信息
    if (db->GetProperty("leveldb.sstables", &stats)) {
        std::cout << "\n2. SST文件详细分析：" << std::endl;
        std::istringstream ss(stats);
        std::string line;
        int current_level = -1;
        
        while (std::getline(ss, line)) {
            if (line.find("level") != std::string::npos) {
                // 修复level解析
                size_t level_pos = line.find("level") + 6; // "level "的长度是6
                while (level_pos < line.length() && line[level_pos] == ' ') {
                    level_pos++; // 跳过额外的空格
                }
                current_level = std::stoi(line.substr(level_pos));
                
                if (current_level == 0) {
                    std::cout << "\nLevel 0 (内存刷盘层)：" << std::endl;
                } else {
                    std::cout << "\nLevel " << current_level << " (排序层)：" << std::endl;
                }
                // 打印表头
                printf("%-6s  %10s  %-15s  %-15s  %-8s  %-8s\n",
                       "文件ID", "大小(KB)", "最小键", "最大键", "最小偏移", "最大偏移");
                printf("----------------------------------------------------------------------\n");
            } else if (!line.empty() && line[0] == ' ') {
                // 解析SST文件信息
                size_t colon_pos = line.find(':');
                size_t bracket_start = line.find('[');
                if (colon_pos != std::string::npos && bracket_start != std::string::npos) {
                    std::string file_id = line.substr(1, colon_pos-1);
                    std::string file_size = line.substr(colon_pos+1, bracket_start-colon_pos-1);
                    std::string key_range = line.substr(bracket_start+1, line.length()-bracket_start-2);
                    
                    // 解析键范围
                    std::string min_key, max_key, min_offset, max_offset;
                    size_t first_quote = key_range.find('\'');
                    size_t second_quote = key_range.find('\'', first_quote+1);
                    size_t third_quote = key_range.find('\'', key_range.find("..")+2);
                    size_t fourth_quote = key_range.find('\'', third_quote+1);
                    
                    if (first_quote != std::string::npos && second_quote != std::string::npos &&
                        third_quote != std::string::npos && fourth_quote != std::string::npos) {
                        min_key = key_range.substr(first_quote+1, second_quote-first_quote-1);
                        max_key = key_range.substr(third_quote+1, fourth_quote-third_quote-1);
                        
                        // 提取偏移量
                        size_t min_offset_start = key_range.find('@') + 2;
                        size_t min_offset_end = key_range.find(':', min_offset_start);
                        size_t max_offset_start = key_range.find('@', min_offset_end) + 2;
                        size_t max_offset_end = key_range.find(':', max_offset_start);
                        
                        min_offset = key_range.substr(min_offset_start, min_offset_end-min_offset_start);
                        max_offset = key_range.substr(max_offset_start, max_offset_end-max_offset_start);
                    }
                    
                    // 计算文件大小（KB）
                    double size_kb = std::stod(file_size) / 1024.0;
                    
                    // 打印格式化的行
                    printf("%-6s  %10.1f  %-15s  %-15s  %-8s  %-8s\n",
                           file_id.c_str(),
                           size_kb,
                           min_key.c_str(),
                           max_key.c_str(),
                           min_offset.c_str(),
                           max_offset.c_str());
                }
            }
        }
    }

    // 3. 显示压缩统计
    if (db->GetProperty("leveldb.stats", &stats)) {
        std::cout << "\n3. 压缩统计：" << std::endl;
        std::istringstream ss(stats);
        std::string line;
        bool in_compaction_stats = false;
        
        std::cout << "Level  文件数  大小(MB)  压缩时间(秒)  读取(MB)  写入(MB)" << std::endl;
        std::cout << "-----------------------------------------------------" << std::endl;
        
        while (std::getline(ss, line)) {
            if (line.find("Compactions") != std::string::npos) {
                in_compaction_stats = true;
                continue;
            }
            if (in_compaction_stats && !line.empty() && line[0] >= '0' && line[0] <= '9') {
                std::istringstream line_ss(line);
                int level, files;
                double size, time, read, write;
                line_ss >> level >> files >> size >> time >> read >> write;
                printf("L%-5d %-8d %-9.1f %-13.1f %-9.1f %-9.1f\n",
                       level, files, size, time, read, write);
            }
        }
    }
}

int main() {
    // 配置数据库选项
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;  // 如果不存在则创建
    options.write_buffer_size = 4 << 10;    // 4KB
    options.max_file_size = 2 << 10;        // 2KB
    options.filter_policy = leveldb::NewBloomFilterPolicy(10);
    options.block_cache = leveldb::NewLRUCache(8 << 20); // 8MB cache

    const std::string db_path = "./tmdb_moviedb";
    bool db_exists = false;

    // 检查数据库是否存在
    {
        leveldb::Options check_options;
        check_options.create_if_missing = false;
        leveldb::Status status = leveldb::DB::Open(check_options, db_path, &db);
        if (status.ok()) {
            std::cout << "=== 发现已存在的数据库 ===" << std::endl;
            db_exists = true;
            // 打开成功，先关闭它
            delete db;
        }
    }

    // 打开数据库
    std::cout << "=== " << (db_exists ? "打开已存在的数据库" : "创建新数据库") << " ===" << std::endl;
    leveldb::Status status = leveldb::DB::Open(options, db_path, &db);
    if (!status.ok()) {
        std::cerr << "无法打开/创建数据库: " << status.ToString() << std::endl;
        return 1;
    }

    // 如果数据库已存在，直接进入查询示例
    if (db_exists) {
        std::cout << "\n数据库已存在，显示当前状态：" << std::endl;
        printDBStats(db);
        
        // 直接跳到遍历示例
        std::cout << "\n=== 遍历数据库示例 ===" << std::endl;
        {
            leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
            int count = 0;
            
            // 从头开始遍历
            std::cout << "\n1. 正向遍历（前5条记录）：" << std::endl;
            for (it->SeekToFirst(); it->Valid() && count < 5; it->Next()) {
                json movie_data = json::parse(it->value().ToString());
                std::cout << "ID: " << it->key().ToString() << std::endl;
                std::cout << "标题: " << movie_data["original_title"] << std::endl;
                std::cout << "导演: " << movie_data["director"] << std::endl;
                std::cout << "上映日期: " << movie_data["release_date"] << std::endl;
                std::cout << "评分: " << movie_data["vote_average"] << std::endl;
                std::cout << "----------------------------------------" << std::endl;
                count++;
            }

            // 从尾部开始遍历
            std::cout << "\n2. 反向遍历（后5条记录）：" << std::endl;
            count = 0;
            for (it->SeekToLast(); it->Valid() && count < 5; it->Prev()) {
                json movie_data = json::parse(it->value().ToString());
                std::cout << "ID: " << it->key().ToString() << std::endl;
                std::cout << "标题: " << movie_data["original_title"] << std::endl;
                std::cout << "导演: " << movie_data["director"] << std::endl;
                std::cout << "上映日期: " << movie_data["release_date"] << std::endl;
                std::cout << "评分: " << movie_data["vote_average"] << std::endl;
                std::cout << "----------------------------------------" << std::endl;
                count++;
            }

            // 范围查询示例（修改为完全匹配）
            std::cout << "\n3. 范围查询（ID 100-105）：" << std::endl;
            for (it->Seek("100"); it->Valid(); it->Next()) {
                std::string current_id = it->key().ToString();
                // 尝试将当前ID转换为整数
                try {
                    int id_num = std::stoi(current_id);
                    // 只处理100到105之间的ID
                    if (id_num >= 100 && id_num <= 105) {
                        json movie_data = json::parse(it->value().ToString());
                        std::cout << "ID: " << current_id << std::endl;
                        std::cout << "标题: " << movie_data["original_title"] << std::endl;
                        std::cout << "导演: " << movie_data["director"] << std::endl;
                        std::cout << "上映日期: " << movie_data["release_date"] << std::endl;
                        std::cout << "----------------------------------------" << std::endl;
                    } else if (id_num > 105) {
                        // 如果ID超过105就退出循环
                        // break; 
                    }
                } catch (const std::exception& e) {
                    // 如果ID不是数字格式，跳过这条记录
                    continue;
                }
            }

            // 前缀查询示例（假设想找ID以"2"开头的电影）
            std::cout << "\n4. 前缀查询（ID以'2'开头，显示前5条）：" << std::endl;
            count = 0;
            for (it->Seek("2"); 
                 it->Valid() && 
                 it->key().ToString().substr(0, 1) == "2" && 
                 count < 5; 
                 it->Next()) {
                json movie_data = json::parse(it->value().ToString());
                std::cout << "ID: " << it->key().ToString() << std::endl;
                std::cout << "标题: " << movie_data["original_title"] << std::endl;
                std::cout << "----------------------------------------" << std::endl;
                count++;
            }

            delete it;
        }
    } else {
        // 如果数据库不存在，执行数据导入流程
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
                
                if (count % 10 == 0) {
                    status = db->Write(leveldb::WriteOptions(), &batch);
                    assert(status.ok());
                    batch.Clear();
                    
                    std::cout << "\n写入 " << count << " 部电影后：" << std::endl;
                    printDBStats(db);
                    
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
    }

    // 清理资源
    delete db;
    delete options.filter_policy;
    delete options.block_cache;

    std::cout << "\n示例运行完成！" << std::endl;
    return 0;
} 