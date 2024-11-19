#include <iostream>
#include <fstream>
#include <sstream>
#include <random>
#include <chrono>
#include <iomanip>
#include <thread>
#include <vector>
#include <map>
#include <unordered_map>
#include <mutex>
#include <set>
#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "leveldb/filter_policy.h"
#include "leveldb/cache.h"
#include "nlohmann/json.hpp"

using json = nlohmann::json;

// 订单数据结构
struct Order {
    std::string order_id;
    std::string user_id;
    std::string product_id;
    double amount;
    std::string status;
    std::string create_time;
    std::string payment_time;
    
    // 序列化为JSON
    std::string serialize() const {
        json j;
        j["order_id"] = order_id;
        j["user_id"] = user_id;
        j["product_id"] = product_id;
        j["amount"] = amount;
        j["status"] = status;
        j["create_time"] = create_time;
        j["payment_time"] = payment_time;
        return j.dump();
    }
    
    // 从JSON反序列化
    static Order deserialize(const std::string& json_str) {
        Order order;
        auto j = json::parse(json_str);
        order.order_id = j["order_id"];
        order.user_id = j["user_id"];
        order.product_id = j["product_id"];
        order.amount = j["amount"];
        order.status = j["status"];
        order.create_time = j["create_time"];
        order.payment_time = j["payment_time"];
        return order;
    }
};

// 验证订单状态转换是否合法
bool isValidStatusTransition(const std::string& old_status, const std::string& new_status) {
    // 定义状态转换规则
    static const std::map<std::string, std::vector<std::string>> valid_transitions = {
        {"pending", {"paid", "cancelled"}},
        {"paid", {"shipped", "cancelled"}},
        {"shipped", {"completed", "cancelled"}},
        {"completed", {}},  // 完成状态不能转换到其他状态
        {"cancelled", {}}   // 取消状态不能转换到其他状态
    };
    
    // 检查旧状态是否存在
    auto it = valid_transitions.find(old_status);
    if (it == valid_transitions.end()) {
        return false;
    }
    
    // 检查新状态是否在允许的转换列表中
    const auto& allowed_states = it->second;
    return std::find(allowed_states.begin(), allowed_states.end(), new_status) 
           != allowed_states.end();
}

// 更新订单状态
bool updateOrderStatus(leveldb::DB* db, const std::string& order_id, 
                      const std::string& new_status, std::string& error_msg) {
    // 获取订单数据
    std::string value;
    leveldb::Status status = db->Get(leveldb::ReadOptions(), "order:" + order_id, &value);
    
    if (!status.ok()) {
        error_msg = "订单不存在";
        return false;
    }
    
    // 反序列化订单数据
    Order order = Order::deserialize(value);
    
    // 验证状态转换
    if (!isValidStatusTransition(order.status, new_status)) {
        error_msg = "非法的状态转换: " + order.status + " -> " + new_status;
        return false;
    }
    
    // 更新状态
    std::string old_status = order.status;
    order.status = new_status;
    
    // 如果是支付状态，更新支付时间
    if (new_status == "paid") {
        auto now = std::chrono::system_clock::now();
        auto tt = std::chrono::system_clock::to_time_t(now);
        std::stringstream ss;
        ss << std::put_time(std::localtime(&tt), "%Y-%m-%d %H:%M:%S");
        order.payment_time = ss.str();
    }
    
    // 使用批处理更新主数据和索引
    leveldb::WriteBatch batch;
    
    // 更新主数据
    batch.Put("order:" + order_id, order.serialize());
    
    // 删除旧状态索引
    batch.Delete("index:status:" + old_status + ":" + order_id);
    
    // 添加新状态索引
    batch.Put("index:status:" + new_status + ":" + order_id, "");
    
    // 执行批处理
    status = db->Write(leveldb::WriteOptions(), &batch);
    
    if (!status.ok()) {
        error_msg = "数据库更新失败: " + status.ToString();
        return false;
    }
    
    return true;
}

// 生成随机订单数据
std::vector<Order> generateOrders(int count) {
    std::vector<Order> orders;
    std::random_device rd;
    std::mt19937 gen(rd());
    
    // 生成随机用户ID (1-10000)
    std::uniform_int_distribution<> user_dist(1, 10000);
    // 生成随机商品ID (1-1000)
    std::uniform_int_distribution<> product_dist(1, 1000);
    // 生成随机金额 (10-1000)
    std::uniform_real_distribution<> amount_dist(10.0, 1000.0);
    // 生成随机状态
    std::vector<std::string> statuses = {"pending", "paid", "shipped", "completed", "cancelled"};
    std::uniform_int_distribution<> status_dist(0, statuses.size() - 1);
    
    // 生成时间范围（最近30天）
    auto now = std::chrono::system_clock::now();
    auto duration = std::chrono::hours(24 * 30);
    std::uniform_int_distribution<> time_dist(0, std::chrono::duration_cast<std::chrono::seconds>(duration).count());
    
    for (int i = 0; i < count; i++) {
        Order order;
        order.order_id = "ORD" + std::to_string(i + 1);
        order.user_id = "U" + std::to_string(user_dist(gen));
        order.product_id = "P" + std::to_string(product_dist(gen));
        order.amount = std::round(amount_dist(gen) * 100) / 100;
        order.status = statuses[status_dist(gen)];
        
        // 生成创建时间
        auto order_time = now - std::chrono::seconds(time_dist(gen));
        auto tt = std::chrono::system_clock::to_time_t(order_time);
        std::stringstream ss;
        ss << std::put_time(std::localtime(&tt), "%Y-%m-%d %H:%M:%S");
        order.create_time = ss.str();
        
        // 如果订单状态不是pending，生成支付时间
        if (order.status != "pending") {
            auto payment_time = order_time + std::chrono::minutes(std::uniform_int_distribution<>(1, 60)(gen));
            tt = std::chrono::system_clock::to_time_t(payment_time);
            ss.str("");
            ss << std::put_time(std::localtime(&tt), "%Y-%m-%d %H:%M:%S");
            order.payment_time = ss.str();
        }
        
        orders.push_back(order);
    }
    
    return orders;
}

// 创建二级索引
void createSecondaryIndex(leveldb::DB* db, const std::string& index_name, 
                         const std::string& order_id, const std::string& index_value) {
    std::string key = index_name + ":" + index_value + ":" + order_id;
    leveldb::Status status = db->Put(leveldb::WriteOptions(), key, "");
}

// 使用二级索引查询
std::vector<Order> queryBySecondaryIndex(leveldb::DB* db, const std::string& index_type,
                                       const std::string& index_value) {
    std::vector<Order> results;
    std::string prefix = "index:" + index_type + ":" + index_value + ":";
    
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(prefix); 
         it->Valid() && it->key().ToString().substr(0, prefix.length()) == prefix;
         it->Next()) {
        // 从索引键中提取订单ID
        std::string order_id = it->key().ToString().substr(prefix.length());
        
        // 查询订单详情（添加 "order:" 前缀）
        std::string value;
        leveldb::Status status = db->Get(leveldb::ReadOptions(), "order:" + order_id, &value);
        if (status.ok()) {
            results.push_back(Order::deserialize(value));
        }
    }
    delete it;
    
    return results;
}

// 添加时间范围查询支持
std::vector<Order> queryByTimeRange(leveldb::DB* db, 
                                  const std::string& start_time, 
                                  const std::string& end_time) {
    std::vector<Order> results;
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    
    std::string prefix = "index:time:";
    std::string start_key = prefix + start_time;
    std::string end_key = prefix + end_time;
    
    for (it->Seek(start_key); 
         it->Valid() && it->key().ToString() < end_key;
         it->Next()) {
        std::string key = it->key().ToString();
        if (key.substr(0, prefix.length()) != prefix) {
            continue;
        }
        
        // 提取订单ID并查询详情
        size_t last_colon = key.find_last_of(':');
        if (last_colon != std::string::npos) {
            std::string order_id = key.substr(last_colon + 1);
            std::string value;
            leveldb::Status status = db->Get(leveldb::ReadOptions(), "order:" + order_id, &value);
            if (status.ok()) {
                results.push_back(Order::deserialize(value));
            }
        }
    }
    delete it;
    
    return results;
}

// 添加金额范围统计
struct AmountStats {
    double total_amount;
    int count;
    double min_amount;
    double max_amount;
    double avg_amount;
    std::map<std::string, double> amount_by_status;

    // 添加构造函数，设置合理的初始值
    AmountStats() : 
        total_amount(0.0),
        count(0),
        min_amount(0.0),  // 将初始值设为0
        max_amount(0.0),
        avg_amount(0.0) {}
};

AmountStats calculateAmountStats(leveldb::DB* db, 
                               const std::string& start_time, 
                               const std::string& end_time) {
    AmountStats stats;
    double sum = 0.0;
    bool first_order = true;  // 添加标志来处理第一个订单
    
    auto orders = queryByTimeRange(db, start_time, end_time);
    for (const auto& order : orders) {
        if (first_order) {
            // 第一个订单，初始化最小和最大值
            stats.min_amount = order.amount;
            stats.max_amount = order.amount;
            first_order = false;
        } else {
            stats.min_amount = std::min(stats.min_amount, order.amount);
            stats.max_amount = std::max(stats.max_amount, order.amount);
        }
        
        sum += order.amount;
        stats.count++;
        stats.amount_by_status[order.status] += order.amount;
    }
    
    stats.total_amount = sum;
    stats.avg_amount = (stats.count > 0) ? (sum / stats.count) : 0.0;
    
    // 如果没有订单，重置最小和最大值为0
    if (stats.count == 0) {
        stats.min_amount = 0.0;
        stats.max_amount = 0.0;
    }
    
    return stats;
}

// 修改用户消费排行函数
std::vector<std::pair<std::string, double>> getUserRanking(leveldb::DB* db, int limit = 10) {
    std::map<std::string, double> user_amounts;
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    
    // 使用order:前缀来只遍历订单数据
    std::string prefix = "order:";
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        if (key.substr(0, prefix.length()) != prefix) {
            break;  // 已经超出order:前缀范围
        }
        
        Order order = Order::deserialize(it->value().ToString());
        if (order.status != "cancelled") {  // 排除已取消的订单
            user_amounts[order.user_id] += order.amount;
        }
    }
    delete it;
    
    // 转换为vector并排序
    std::vector<std::pair<std::string, double>> ranking(
        user_amounts.begin(), user_amounts.end());
    std::sort(ranking.begin(), ranking.end(),
              [](const std::pair<std::string, double>& a, 
                 const std::pair<std::string, double>& b) { 
                  return a.second > b.second; 
              });
    
    // 返回前N名
    if (ranking.size() > limit) {
        ranking.resize(limit);
    }
    
    return ranking;
}

// 修改商品热度排行函数
std::vector<std::pair<std::string, int>> getProductRanking(leveldb::DB* db, int limit = 10) {
    std::map<std::string, int> product_counts;
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    
    // 使用order:前缀来只遍历订单数据
    std::string prefix = "order:";
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        if (key.substr(0, prefix.length()) != prefix) {
            break;  // 已经超出order:前缀范围
        }
        
        Order order = Order::deserialize(it->value().ToString());
        if (order.status != "cancelled") {
            product_counts[order.product_id]++;
        }
    }
    delete it;
    
    std::vector<std::pair<std::string, int>> ranking(
        product_counts.begin(), product_counts.end());
    std::sort(ranking.begin(), ranking.end(),
              [](const std::pair<std::string, int>& a, 
                 const std::pair<std::string, int>& b) { 
                  return a.second > b.second; 
              });
    
    if (ranking.size() > limit) {
        ranking.resize(limit);
    }
    
    return ranking;
}

// 添加 LSM 树状态分析函数
void printLSMTreeStatus(leveldb::DB* db) {
    std::string stats;
    
    std::cout << "\n=== LevelDB LSM树状态分析 ===" << std::endl;
    
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
                    explanation = "内存表盘区域，文件可能有重叠";
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
                size_t level_pos = line.find("level") + 6;
                while (level_pos < line.length() && line[level_pos] == ' ') {
                    level_pos++;
                }
                current_level = std::stoi(line.substr(level_pos));
                
                if (current_level == 0) {
                    std::cout << "\nLevel 0 (内存刷盘层)：" << std::endl;
                } else {
                    std::cout << "\nLevel " << current_level << " (排序层)：" << std::endl;
                }
                printf("%-6s  %10s  %-15s  %-15s  %-8s  %-8s\n",
                       "文件ID", "大小(KB)", "最小键", "最大键", "最小偏移", "最大偏移");
                printf("----------------------------------------------------------------------\n");
            } else if (!line.empty() && line[0] == ' ') {
                size_t colon_pos = line.find(':');
                size_t bracket_start = line.find('[');
                if (colon_pos != std::string::npos && bracket_start != std::string::npos) {
                    std::string file_id = line.substr(1, colon_pos-1);
                    std::string file_size = line.substr(colon_pos+1, bracket_start-colon_pos-1);
                    std::string key_range = line.substr(bracket_start+1, line.length()-bracket_start-2);
                    
                    // 解析键范
                    std::string min_key, max_key, min_offset, max_offset;
                    size_t first_quote = key_range.find('\'');
                    size_t second_quote = key_range.find('\'', first_quote+1);
                    size_t third_quote = key_range.find('\'', key_range.find("..")+2);
                    size_t fourth_quote = key_range.find('\'', third_quote+1);
                    
                    if (first_quote != std::string::npos && second_quote != std::string::npos &&
                        third_quote != std::string::npos && fourth_quote != std::string::npos) {
                        min_key = key_range.substr(first_quote+1, second_quote-first_quote-1);
                        max_key = key_range.substr(third_quote+1, fourth_quote-third_quote-1);
                        
                        size_t min_offset_start = key_range.find('@') + 2;
                        size_t min_offset_end = key_range.find(':', min_offset_start);
                        size_t max_offset_start = key_range.find('@', min_offset_end) + 2;
                        size_t max_offset_end = key_range.find(':', max_offset_start);
                        
                        min_offset = key_range.substr(min_offset_start, min_offset_end-min_offset_start);
                        max_offset = key_range.substr(max_offset_start, max_offset_end-max_offset_start);
                    }
                    
                    // 算文件大小（KB）
                    double size_kb = std::stod(file_size) / 1024.0;
                    
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

// 在 Order 结构体后添加状态变更记录结构
struct StatusChangeRecord {
    std::string order_id;
    std::string old_status;
    std::string new_status;
    std::string change_time;
    std::string operator_id;  // 可选：记录操作者ID
    
    // 序列化为JSON
    std::string serialize() const {
        json j;
        j["order_id"] = order_id;
        j["old_status"] = old_status;
        j["new_status"] = new_status;
        j["change_time"] = change_time;
        j["operator_id"] = operator_id;
        return j.dump();
    }
    
    static StatusChangeRecord deserialize(const std::string& json_str) {
        StatusChangeRecord record;
        auto j = json::parse(json_str);
        record.order_id = j["order_id"];
        record.old_status = j["old_status"];
        record.new_status = j["new_status"];
        record.change_time = j["change_time"];
        record.operator_id = j["operator_id"];
        return record;
    }
};

// 添加批量状态更新函数
struct BatchUpdateResult {
    std::vector<std::string> success_orders;
    std::map<std::string, std::string> failed_orders;  // order_id -> error_message
};

BatchUpdateResult batchUpdateOrderStatus(
    leveldb::DB* db,
    const std::vector<std::string>& order_ids,
    const std::string& new_status,
    const std::string& operator_id = "SYSTEM"
) {
    BatchUpdateResult result;
    leveldb::WriteBatch batch;
    std::vector<StatusChangeRecord> changes;
    
    auto now = std::chrono::system_clock::now();
    auto tt = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << std::put_time(std::localtime(&tt), "%Y-%m-%d %H:%M:%S");
    std::string current_time = ss.str();
    
    // 第一阶段：验证所有订单
    for (const auto& order_id : order_ids) {
        std::string value;
        leveldb::Status status = db->Get(leveldb::ReadOptions(), "order:" + order_id, &value);
        
        if (!status.ok()) {
            result.failed_orders[order_id] = "订单不存在";
            continue;
        }
        
        Order order = Order::deserialize(value);
        if (!isValidStatusTransition(order.status, new_status)) {
            result.failed_orders[order_id] = "非法的状态转换: " + order.status + " -> " + new_status;
            continue;
        }
        
        // 记录成功的订单和状态变更
        result.success_orders.push_back(order_id);
        
        // 准备状态变更记录
        StatusChangeRecord change_record;
        change_record.order_id = order_id;
        change_record.old_status = order.status;
        change_record.new_status = new_status;
        change_record.change_time = current_time;
        change_record.operator_id = operator_id;
        changes.push_back(change_record);
        
        // 更新订单状态
        order.status = new_status;
        if (new_status == "paid") {
            order.payment_time = current_time;
        }
        
        // 添加到批处理
        batch.Put("order:" + order_id, order.serialize());
        batch.Delete("index:status:" + change_record.old_status + ":" + order_id);
        batch.Put("index:status:" + new_status + ":" + order_id, "");
        
        // 添加状态变更记录
        std::string change_key = "status_change:" + order_id + ":" + current_time;
        batch.Put(change_key, change_record.serialize());
    }
    
    // 第二阶段：执行批量更新
    if (!result.success_orders.empty()) {
        leveldb::Status write_status = db->Write(leveldb::WriteOptions(), &batch);
        if (!write_status.ok()) {
            // 如果写入失败，将所有订单标记失败
            for (const auto& order_id : result.success_orders) {
                result.failed_orders[order_id] = "数据库写入失败: " + write_status.ToString();
            }
            result.success_orders.clear();
        }
    }
    
    return result;
}

// 添加查询订单状态历史记录的函数
std::vector<StatusChangeRecord> getOrderStatusHistory(
    leveldb::DB* db,
    const std::string& order_id
) {
    std::vector<StatusChangeRecord> history;
    std::string prefix = "status_change:" + order_id + ":";
    
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(prefix); 
         it->Valid() && it->key().ToString().substr(0, prefix.length()) == prefix;
         it->Next()) {
        history.push_back(StatusChangeRecord::deserialize(it->value().ToString()));
    }
    delete it;
    
    // 按时间排序
    std::sort(history.begin(), history.end(),
              [](const StatusChangeRecord& a, const StatusChangeRecord& b) {
                  return a.change_time < b.change_time;
              });
    
    return history;
}

// 添加订单备注功能
struct OrderNote {
    std::string note_id;
    std::string order_id;
    std::string content;
    std::string create_time;
    std::string creator_id;
    
    std::string serialize() const {
        json j;
        j["note_id"] = note_id;
        j["order_id"] = order_id;
        j["content"] = content;
        j["create_time"] = create_time;
        j["creator_id"] = creator_id;
        return j.dump();
    }
    
    static OrderNote deserialize(const std::string& json_str) {
        OrderNote note;
        auto j = json::parse(json_str);
        note.note_id = j["note_id"];
        note.order_id = j["order_id"];
        note.content = j["content"];
        note.create_time = j["create_time"];
        note.creator_id = j["creator_id"];
        return note;
    }
};

// 完善退款请求结构体
struct RefundRequest {
    std::string refund_id;
    std::string order_id;
    double amount;
    std::string reason;
    std::string status;  // pending, approved, rejected
    std::string create_time;
    std::string process_time;
    std::string processor_id;
    
    // 序列化为JSON
    std::string serialize() const {
        json j;
        j["refund_id"] = refund_id;
        j["order_id"] = order_id;
        j["amount"] = amount;
        j["reason"] = reason;
        j["status"] = status;
        j["create_time"] = create_time;
        j["process_time"] = process_time;
        j["processor_id"] = processor_id;
        return j.dump();
    }
    
    static RefundRequest deserialize(const std::string& json_str) {
        RefundRequest refund;
        auto j = json::parse(json_str);
        refund.refund_id = j["refund_id"];
        refund.order_id = j["order_id"];
        refund.amount = j["amount"];
        refund.reason = j["reason"];
        refund.status = j["status"];
        refund.create_time = j["create_time"];
        refund.process_time = j["process_time"];
        refund.processor_id = j["processor_id"];
        return refund;
    }
};

// 添加退款管理类
class RefundManager {
public:
    // 创建退款申请
    static bool createRefundRequest(
        leveldb::DB* db,
        const std::string& order_id,
        double amount,
        const std::string& reason,
        std::string& error_msg
    ) {
        // 检查订单是否存在
        std::string value;
        leveldb::Status status = db->Get(leveldb::ReadOptions(), "order:" + order_id, &value);
        if (!status.ok()) {
            error_msg = "订单不存在";
            return false;
        }
        
        Order order = Order::deserialize(value);
        
        // 检查订单状态是否允许退款
        if (order.status == "pending" || order.status == "cancelled") {
            error_msg = "当前订单状态不允许退款";
            return false;
        }
        
        // 检查退款金额
        if (amount <= 0 || amount > order.amount) {
            error_msg = "退款金额无效";
            return false;
        }
        
        // 创建退款请求
        RefundRequest refund;
        refund.refund_id = "REF" + order_id + "_" + std::to_string(std::time(nullptr));
        refund.order_id = order_id;
        refund.amount = amount;
        refund.reason = reason;
        refund.status = "pending";
        
        auto now = std::chrono::system_clock::now();
        auto tt = std::chrono::system_clock::to_time_t(now);
        std::stringstream ss;
        ss << std::put_time(std::localtime(&tt), "%Y-%m-%d %H:%M:%S");
        refund.create_time = ss.str();
        
        // 写入数据库
        leveldb::WriteBatch batch;
        batch.Put("refund:" + refund.refund_id, refund.serialize());
        batch.Put("index:refund:order:" + order_id + ":" + refund.refund_id, "");
        batch.Put("index:refund:status:pending:" + refund.refund_id, "");
        
        status = db->Write(leveldb::WriteOptions(), &batch);
        if (!status.ok()) {
            error_msg = "数据库写入失败: " + status.ToString();
            return false;
        }
        
        return true;
    }
    
    // 处理退款申请
    static bool processRefundRequest(
        leveldb::DB* db,
        const std::string& refund_id,
        bool approved,
        const std::string& processor_id,
        std::string& error_msg
    ) {
        // 获取退款申请
        std::string value;
        leveldb::Status status = db->Get(leveldb::ReadOptions(), "refund:" + refund_id, &value);
        if (!status.ok()) {
            error_msg = "退款申请不存在";
            return false;
        }
        
        RefundRequest refund = RefundRequest::deserialize(value);
        if (refund.status != "pending") {
            error_msg = "退款申请已处理";
            return false;
        }
        
        // 更新退款状态
        refund.status = approved ? "approved" : "rejected";
        refund.processor_id = processor_id;
        
        auto now = std::chrono::system_clock::now();
        auto tt = std::chrono::system_clock::to_time_t(now);
        std::stringstream ss;
        ss << std::put_time(std::localtime(&tt), "%Y-%m-%d %H:%M:%S");
        refund.process_time = ss.str();
        
        // 如果批准退款，更新订单状态
        leveldb::WriteBatch batch;
        if (approved) {
            // 获取订单信息
            status = db->Get(leveldb::ReadOptions(), "order:" + refund.order_id, &value);
            if (!status.ok()) {
                error_msg = "订单不存在";
                return false;
            }
            
            Order order = Order::deserialize(value);
            order.status = "cancelled";  // 或创建新的退款完成状态
            
            batch.Put("order:" + order.order_id, order.serialize());
            // 更新订单状态索引
            batch.Delete("index:status:" + order.status + ":" + order.order_id);
            batch.Put("index:status:cancelled:" + order.order_id, "");
        }
        
        // 更新退款记录和索引
        batch.Put("refund:" + refund.refund_id, refund.serialize());
        batch.Delete("index:refund:status:pending:" + refund.refund_id);
        batch.Put("index:refund:status:" + refund.status + ":" + refund.refund_id, "");
        
        status = db->Write(leveldb::WriteOptions(), &batch);
        if (!status.ok()) {
            error_msg = "数据库更新失败: " + status.ToString();
            return false;
        }
        
        return true;
    }
    
    // 查询退款申请
    static std::vector<RefundRequest> queryRefundsByOrder(
        leveldb::DB* db,
        const std::string& order_id
    ) {
        std::vector<RefundRequest> refunds;
        std::string prefix = "index:refund:order:" + order_id + ":";
        
        leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
        for (it->Seek(prefix); 
             it->Valid() && it->key().ToString().substr(0, prefix.length()) == prefix;
             it->Next()) {
            std::string refund_id = it->key().ToString().substr(prefix.length());
            std::string value;
            leveldb::Status status = db->Get(leveldb::ReadOptions(), "refund:" + refund_id, &value);
            if (status.ok()) {
                refunds.push_back(RefundRequest::deserialize(value));
            }
        }
        delete it;
        
        return refunds;
    }
    
    // 查询特定状态的退款申请
    static std::vector<RefundRequest> queryRefundsByStatus(
        leveldb::DB* db,
        const std::string& status
    ) {
        std::vector<RefundRequest> refunds;
        std::string prefix = "index:refund:status:" + status + ":";
        
        leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
        for (it->Seek(prefix); 
             it->Valid() && it->key().ToString().substr(0, prefix.length()) == prefix;
             it->Next()) {
            std::string refund_id = it->key().ToString().substr(prefix.length());
            std::string value;
            leveldb::Status status = db->Get(leveldb::ReadOptions(), "refund:" + refund_id, &value);
            if (status.ok()) {
                refunds.push_back(RefundRequest::deserialize(value));
            }
        }
        delete it;
        
        return refunds;
    }
};

// 修改缓存管理类的实现
class OrderCache {
private:
    // LRU 缓存节点结构
    struct CacheNode {
        Order order;
        std::string key;
        CacheNode* prev;
        CacheNode* next;
        
        CacheNode(const std::string& k, const Order& o) 
            : order(o), key(k), prev(nullptr), next(nullptr) {}
    };
    
    // 双向链表操作
    void moveToFront(CacheNode* node) {
        if (node == head) return;
        
        // 从当前位置移除
        node->prev->next = node->next;
        if (node->next) {
            node->next->prev = node->prev;
        } else {
            tail = node->prev;
        }
        
        // 移到头部
        node->next = head;
        node->prev = nullptr;
        head->prev = node;
        head = node;
    }
    
    void addToFront(CacheNode* node) {
        if (!head) {
            head = tail = node;
        } else {
            node->next = head;
            head->prev = node;
            head = node;
        }
    }
    
    void removeTail() {
        if (!tail) return;
        
        CacheNode* oldTail = tail;
        tail = tail->prev;
        if (tail) {
            tail->next = nullptr;
        } else {
            head = nullptr;
        }
        
        cache_map.erase(oldTail->key);
        delete oldTail;
        current_size--;
    }
    
    // 缓存成员变量
    std::unordered_map<std::string, CacheNode*> cache_map;
    CacheNode* head;
    CacheNode* tail;
    size_t max_size;
    size_t current_size;
    mutable std::mutex mutex;
    
    // 缓存统计
    struct CacheStats {
        size_t hits;
        size_t misses;
        size_t evictions;
    } stats;
    
    // 添加一个内部的 put 方法，不加锁
    void putInternal(const std::string& order_id, const Order& order) {
        auto it = cache_map.find(order_id);
        if (it != cache_map.end()) {
            // 更新现有节点
            it->second->order = order;
            moveToFront(it->second);
        } else {
            // 创建新节点
            CacheNode* node = new CacheNode(order_id, order);
            
            // 如果缓存已满，移除最久未使用的项
            if (current_size >= max_size) {
                removeTail();
                stats.evictions++;
            }
            
            addToFront(node);
            cache_map[order_id] = node;
            current_size++;
        }
    }

public:
    OrderCache(size_t size = 1000) 
        : max_size(size), current_size(0), head(nullptr), tail(nullptr) {
        stats = {0, 0, 0};
    }
    
    ~OrderCache() {
        clear();
    }
    
    // 添加或更新缓存
    void put(const std::string& order_id, const Order& order) {
        std::lock_guard<std::mutex> lock(mutex);
        
        auto it = cache_map.find(order_id);
        if (it != cache_map.end()) {
            // 更新现有节点
            it->second->order = order;
            moveToFront(it->second);
        } else {
            // 创建新节点
            CacheNode* node = new CacheNode(order_id, order);
            
            // 如果缓存已满，移除最久未使用的项
            if (current_size >= max_size) {
                removeTail();
                stats.evictions++;
            }
            
            addToFront(node);
            cache_map[order_id] = node;
            current_size++;
        }
    }
    
    // 获取缓存项
    bool get(const std::string& order_id, Order& order) {
        std::lock_guard<std::mutex> lock(mutex);
        
        auto it = cache_map.find(order_id);
        if (it != cache_map.end()) {
            order = it->second->order;
            moveToFront(it->second);
            stats.hits++;
            return true;
        }
        
        stats.misses++;
        return false;
    }
    
    // 移除缓存项
    void remove(const std::string& order_id) {
        std::lock_guard<std::mutex> lock(mutex);
        
        auto it = cache_map.find(order_id);
        if (it != cache_map.end()) {
            CacheNode* node = it->second;
            
            if (node->prev) {
                node->prev->next = node->next;
            } else {
                head = node->next;
            }
            
            if (node->next) {
                node->next->prev = node->prev;
            } else {
                tail = node->prev;
            }
            
            cache_map.erase(it);
            delete node;
            current_size--;
        }
    }
    
    // 清空缓存
    void clear() {
        std::lock_guard<std::mutex> lock(mutex);
        
        for (auto& pair : cache_map) {
            delete pair.second;
        }
        cache_map.clear();
        head = tail = nullptr;
        current_size = 0;
    }
    
    // 取缓存统计信息
    std::string getStats() const {
        std::lock_guard<std::mutex> lock(mutex);
        
        std::stringstream ss;
        ss << "Cache Statistics:\n"
           << "  Size: " << current_size << "/" << max_size << "\n"
           << "  Hits: " << stats.hits << "\n"
           << "  Misses: " << stats.misses << "\n"
           << "  Hit Rate: " << (stats.hits + stats.misses == 0 ? 0.0 :
                                static_cast<double>(stats.hits) / (stats.hits + stats.misses) * 100)
           << "%\n"
           << "  Evictions: " << stats.evictions;
        return ss.str();
    }
    
    // 添加进度回调函数类型
    using ProgressCallback = std::function<void(size_t current, size_t total)>;
    
    // 修改 warmup 方法，添加进度回调
    void warmup(leveldb::DB* db, size_t limit = 1000, 
                ProgressCallback progress_cb = nullptr) {
        std::cout << "开始预热缓存..." << std::endl;
        
        // 先计算总订单数
        size_t total_orders = 0;
        {
            leveldb::ReadOptions count_options;
            count_options.fill_cache = false;
            std::unique_ptr<leveldb::Iterator> it(db->NewIterator(count_options));
            for (it->SeekToFirst(); it->Valid(); it->Next()) {
                if (it->key().ToString().substr(0, 6) == "order:") {
                    total_orders++;
                }
            }
        }
        
        if (total_orders == 0) {
            std::cout << "数据库中没有订单数据" << std::endl;
            return;
        }
        
        // 限制加载数量
        size_t target_count = std::min(limit, total_orders);
        std::cout << "计划加载 " << target_count << " / " << total_orders 
                 << " 条订单到缓存" << std::endl;
        
        // 收集订单数据
        std::vector<std::pair<std::string, Order>> orders_to_cache;
        size_t processed = 0;
        size_t errors = 0;
        
        {
            leveldb::ReadOptions read_options;
            read_options.fill_cache = false;
            std::unique_ptr<leveldb::Iterator> it(db->NewIterator(read_options));
            
            for (it->SeekToFirst(); it->Valid() && processed < target_count; it->Next()) {
                try {
                    std::string key = it->key().ToString();
                    if (key.substr(0, 6) != "order:") {
                        continue;
                    }
                    
                    std::string order_id = key.substr(6);
                    Order order = Order::deserialize(it->value().ToString());
                    orders_to_cache.emplace_back(order_id, order);
                    processed++;
                    
                    if (progress_cb) {
                        progress_cb(processed, target_count);
                    }
                    
                } catch (const std::exception& e) {
                    std::cerr << "处理订单时发生错误: " << e.what() << std::endl;
                    errors++;
                    if (errors > target_count / 10) {  // 如果错误率超过10%
                        throw std::runtime_error("错误率过高，中止缓存预热");
                    }
                }
            }
        }
        
        // 批量添加到缓存
        {
            std::lock_guard<std::mutex> lock(mutex);
            size_t cached = 0;
            
            for (const auto& [order_id, order] : orders_to_cache) {
                putInternal(order_id, order);
                cached++;
                
                if (progress_cb) {
                    progress_cb(cached, orders_to_cache.size());
                }
            }
        }
        
        std::cout << "\n缓存预热完成：" << std::endl;
        std::cout << "- 成功缓存: " << current_size << " 条订单" << std::endl;
        std::cout << "- 处理失败: " << errors << " 条订单" << std::endl;
        std::cout << getStats() << std::endl;
    }
};

// 添加数据导出功能
class OrderExporter {
public:
    static bool exportToCSV(leveldb::DB* db, const std::string& filename,
                           const std::string& start_time, const std::string& end_time) {
        std::ofstream file(filename);
        if (!file.is_open()) {
            return false;
        }
        
        // 写入CSV头
        file << "订单ID,用户ID,商品ID,金额,状态,创建时间,支付时间\n";
        
        // 查询并写入数据
        auto orders = queryByTimeRange(db, start_time, end_time);
        for (const auto& order : orders) {
            file << order.order_id << ","
                 << order.user_id << ","
                 << order.product_id << ","
                 << order.amount << ","
                 << order.status << ","
                 << order.create_time << ","
                 << order.payment_time << "\n";
        }
        
        return true;
    }
};

// 添加按金额范围查询功能
std::vector<Order> queryByAmountRange(leveldb::DB* db, 
                                    double min_amount, 
                                    double max_amount) {
    std::vector<Order> results;
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    
    // 使用order:前缀来只遍历订单数据
    std::string prefix = "order:";
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        if (key.substr(0, prefix.length()) != prefix) {
            break;  // 已经超出order:前缀范围
        }
        
        Order order = Order::deserialize(it->value().ToString());
        if (order.amount >= min_amount && order.amount <= max_amount) {
            results.push_back(order);
        }
    }
    delete it;
    
    return results;
}


// 添加监控告警功能
class OrderMonitor {
public:
    struct AlertConfig {
        double max_order_amount;
        int max_pending_orders;
        int max_cancelled_orders_per_hour;
    };
    
    static void checkAlerts(leveldb::DB* db, const AlertConfig& config) {
        // 检查异常大额订单
        auto large_orders = queryByAmountRange(db, config.max_order_amount, 
                                             std::numeric_limits<double>::max());
        if (!large_orders.empty()) {
            std::cout << "警告：发现" << large_orders.size() 
                     << "笔异常大额订单！" << std::endl;
        }
        
        // 检查待支付订单堆积
        auto pending_orders = queryBySecondaryIndex(db, "status", "pending");
        if (pending_orders.size() > config.max_pending_orders) {
            std::cout << "警告：待支付订单堆积("
                     << pending_orders.size() << "笔)！" << std::endl;
        }
        
        // 检查取消订单率
        auto now = std::chrono::system_clock::now();
        auto one_hour_ago = now - std::chrono::hours(1);
        // ... 实现统计逻辑
    }
};

// 添加数据一致性检查功能
class OrderConsistencyChecker {
public:
    static void checkIndexConsistency(leveldb::DB* db) {
        std::map<std::string, std::set<std::string>> status_index;
        std::map<std::string, Order> orders;
        
        // 收集所有订单数据
        leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
        for (it->Seek("order:"); it->Valid(); it->Next()) {
            std::string key = it->key().ToString();
            if (key.substr(0, 6) != "order:") break;
            
            Order order = Order::deserialize(it->value().ToString());
            orders[order.order_id] = order;
        }
        delete it;
        
        // 检查索引一致性
        it = db->NewIterator(leveldb::ReadOptions());
        for (it->Seek("index:"); it->Valid(); it->Next()) {
            std::string key = it->key().ToString();
            if (key.substr(0, 6) != "index:") break;
            
            // 解析索引键
            // ... 实现检查逻辑
        }
        delete it;
    }
};

// 在 main 函数中添加缓存测试代码
void testCache(leveldb::DB* db) {
    std::cout << "\n10. 测试缓存功能：" << std::endl;
    
    // 创建缓存实例
    OrderCache cache(1000);  // 设置缓存大小为1000
    
    // 先确保数据库中有足够的测试数据
    std::cout << "检查数据库中的订单数量..." << std::endl;
    size_t order_count = 0;
    {
        leveldb::ReadOptions read_options;
        read_options.fill_cache = false;
        leveldb::Iterator* it = db->NewIterator(read_options);
        
        std::cout << "开始遍历数据库..." << std::endl;
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::string key = it->key().ToString();
            if (key.substr(0, 6) == "order:") {
                order_count++;
                if (order_count % 1000 == 0) {
                    std::cout << "已扫描 " << order_count << " 条订单..." << std::endl;
                }
            }
        }
        
        if (!it->status().ok()) {
            std::cerr << "遍历数据库时发生错误: " << it->status().ToString() << std::endl;
        }
        
        delete it;
    }
    std::cout << "数据库中共有 " << order_count << " 条订单" << std::endl;
    
    if (order_count == 0) {
        std::cout << "生成测试数据..." << std::endl;
        auto test_orders = generateOrders(1000);  // 生成1000条测试订单
        for (const auto& order : test_orders) {
            leveldb::Status status = db->Put(leveldb::WriteOptions(), 
                                           "order:" + order.order_id, 
                                           order.serialize());
            if (!status.ok()) {
                std::cerr << "写入订单数据失败: " << status.ToString() << std::endl;
            }
        }
        std::cout << "已生成 " << test_orders.size() << " 条测试订单" << std::endl;
        order_count = test_orders.size();
    }
    
    // 添加进度显示
    auto progress_callback = [](size_t current, size_t total) {
        static size_t last_percent = 0;
        size_t percent = (current * 100) / total;
        if (percent != last_percent) {
            std::cout << "\r预热进度: " << percent << "% (" 
                     << current << "/" << total << ")" << std::flush;
            last_percent = percent;
        }
    };
    
    // 使用进度回调预热缓存
    try {
        cache.warmup(db, std::min(size_t(500), order_count), progress_callback);
        std::cout << std::endl;  // 换行
    } catch (const std::exception& e) {
        std::cerr << "\n缓存预热失败: " << e.what() << std::endl;
    }
    
    // 添加性能测试
    std::cout << "\n执行缓存性能测试：" << std::endl;
    
    // 准备测试数据
    std::vector<std::string> test_order_ids;
    {
        leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
        for (it->SeekToFirst(); it->Valid() && test_order_ids.size() < 1000; it->Next()) {
            std::string key = it->key().ToString();
            if (key.substr(0, 6) == "order:") {
                test_order_ids.push_back(key.substr(6));
            }
        }
        delete it;
    }
    
    if (test_order_ids.empty()) {
        std::cout << "没有可用的测试数据" << std::endl;
        return;
    }
    
    // 测试缓存读取性能
    {
        std::cout << "\n测试缓存读取性能 (1000次随机读取)：" << std::endl;
        
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, test_order_ids.size() - 1);
        
        auto start_time = std::chrono::high_resolution_clock::now();
        size_t hits = 0;
        
        for (int i = 0; i < 1000; i++) {
            Order order;
            std::string order_id = test_order_ids[dis(gen)];
            if (cache.get(order_id, order)) {
                hits++;
            }
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            end_time - start_time);
        
        std::cout << "总耗时: " << duration.count() / 1000.0 << "ms" << std::endl;
        std::cout << "平均耗时: " << duration.count() / 1000.0 << "微秒/次" << std::endl;
        std::cout << "缓存命中率: " << (hits * 100.0 / 1000) << "%" << std::endl;
    }
    
    // 测试缓存写入性能
    {
        std::cout << "\n测试缓存写入性能 (1000次随机写入)：" << std::endl;
        
        auto start_time = std::chrono::high_resolution_clock::now();
        
        for (int i = 0; i < 1000; i++) {
            Order order;
            order.order_id = "PERF_TEST_" + std::to_string(i);
            cache.put(order.order_id, order);
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            end_time - start_time);
        
        std::cout << "总耗时: " << duration.count() / 1000.0 << "ms" << std::endl;
        std::cout << "平均耗时: " << duration.count() / 1000.0 << "微秒/次" << std::endl;
    }
}

int main() {
    // 配置数据库选项
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 64 << 20;  // 64MB
    options.max_file_size = 4 << 20;       // 4MB
    options.filter_policy = leveldb::NewBloomFilterPolicy(10);
    options.block_cache = leveldb::NewLRUCache(8 << 20); // 8MB cache

    // 打开数据库
    const std::string db_path = "./orderdb";
    leveldb::Status status = leveldb::DB::Open(options, db_path, &db);
    if (!status.ok()) {
        std::cerr << "无法打开数据库: " << status.ToString() << std::endl;
        return 1;
    }

    // 生成示例订单数据
    std::cout << "生成订单数据..." << std::endl;
    auto orders = generateOrders(100000);  // 生成10万条订单
    std::cout << "生成了 " << orders.size() << " 条订单数据" << std::endl;

    // 批量写入数据并创建索引
    std::cout << "\n写入数据并创建索引..." << std::endl;
    {
        // 分别创建两个批次，一个用于主数据，一个用于索引
        leveldb::WriteBatch main_batch;
        leveldb::WriteBatch index_batch;
        int count = 0;
        
        for (const auto& order : orders) {
            // 主数据使用 "order:"前缀
            std::string main_key = "order:" + order.order_id;
            main_batch.Put(main_key, order.serialize());
            
            // 索引使用 "index:"前缀
            std::string user_index_key = "index:user:" + order.user_id + ":" + order.order_id;
            std::string status_index_key = "index:status:" + order.status + ":" + order.order_id;
            std::string time_index_key = "index:time:" + order.create_time + ":" + order.order_id;
            
            index_batch.Put(user_index_key, "");
            index_batch.Put(status_index_key, "");
            index_batch.Put(time_index_key, "");
            
            count++;
            if (count % 1000 == 0) {
                // 分别写入主数据和索引数据
                status = db->Write(leveldb::WriteOptions(), &main_batch);
                assert(status.ok());
                status = db->Write(leveldb::WriteOptions(), &index_batch);
                assert(status.ok());
                
                main_batch.Clear();
                index_batch.Clear();
                
                std::cout << "已写入 " << count << " 条数据..." << std::endl;
            }
        }
        
        // 写入剩余数据
        if (status.ok()) {
            status = db->Write(leveldb::WriteOptions(), &main_batch);
            assert(status.ok());
            status = db->Write(leveldb::WriteOptions(), &index_batch);
            assert(status.ok());
        }
    }

    // 在写入数据后添加 LSM 树状态分析
    std::cout << "\n=== 数据写入完成，分析 LSM 树状态 ===" << std::endl;
    printLSMTreeStatus(db);

    // 在执行查询前触发一次手动压缩
    std::cout << "\n=== 触发手动压缩 ===" << std::endl;
    db->CompactRange(nullptr, nullptr);
    
    // 显示压缩后的状态
    std::cout << "\n=== 压缩后的 LSM 树状态 ===" << std::endl;
    printLSMTreeStatus(db);

    // 示例查询
    std::cout << "\n=== 查询示例 ===" << std::endl;
    
    // 1. 按用户ID查询订单
    std::cout << "\n1. 查询用户 U100 的订单：" << std::endl;
    auto user_orders = queryBySecondaryIndex(db, "user", "U100");
    std::cout << "找到 " << user_orders.size() << " 条订单" << std::endl;
    for (const auto& order : user_orders) {
        std::cout << "订单ID: " << order.order_id 
                 << ", 金额: " << order.amount 
                 << ", 状态: " << order.status << std::endl;
    }
    
    // 2. 按状态查询订单
    std::cout << "\n2. 查询状态为 'pending' 的订单（显示前5条）：" << std::endl;
    auto pending_orders = queryBySecondaryIndex(db, "status", "pending");
    std::cout << "找到 " << pending_orders.size() << " 条待支付订单" << std::endl;
    for (size_t i = 0; i < std::min(size_t(5), pending_orders.size()); i++) {
        const auto& order = pending_orders[i];
        std::cout << "订单ID: " << order.order_id 
                 << ", 用户: " << order.user_id 
                 << ", 金额: " << order.amount << std::endl;
    }
    
    // 3. 统计各状态订单数量
    std::cout << "\n3. 订单状态统计：" << std::endl;
    std::map<std::string, int> status_counts;
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    std::string status_prefix = "index:status:";
    for (it->Seek(status_prefix); 
         it->Valid() && it->key().ToString().substr(0, status_prefix.length()) == status_prefix;
         it->Next()) {
        std::string key = it->key().ToString();
        size_t first_colon = key.find(':', status_prefix.length());
        if (first_colon != std::string::npos) {
            std::string status = key.substr(status_prefix.length(), 
                                          first_colon - status_prefix.length());
            status_counts[status]++;
        }
    }
    delete it;
    
    for (const auto& pair : status_counts) {
        std::cout << std::setw(10) << pair.first << ": " << pair.second << " 条" << std::endl;
    }

    // 添加更多查询示例
    std::cout << "\n=== 高级查询示例 ===" << std::endl;
    
    // 4. 时间范围查询
    std::cout << "\n4. 最近24小时订单统计：" << std::endl;
    auto current_time = std::chrono::system_clock::now();
    auto yesterday = current_time - std::chrono::hours(24);
    auto tt_now = std::chrono::system_clock::to_time_t(current_time);
    auto tt_yesterday = std::chrono::system_clock::to_time_t(yesterday);
    
    std::stringstream ss_now, ss_yesterday;
    ss_now << std::put_time(std::localtime(&tt_now), "%Y-%m-%d %H:%M:%S");
    ss_yesterday << std::put_time(std::localtime(&tt_yesterday), "%Y-%m-%d %H:%M:%S");
    
    auto stats = calculateAmountStats(db, ss_yesterday.str(), ss_now.str());
    std::cout << "订单总数: " << stats.count << std::endl;
    std::cout << std::fixed << std::setprecision(2);  // 设置输出精度为2位小数
    std::cout << "总金额: ￥" << stats.total_amount << std::endl;
    if (stats.count > 0) {
        std::cout << "平均订单金额: ￥" << stats.avg_amount << std::endl;
        std::cout << "最小订单金额: ￥" << stats.min_amount << std::endl;
        std::cout << "最大订单金额: ￥" << stats.max_amount << std::endl;
    } else {
        std::cout << "该时间段内没有订单" << std::endl;
    }
    
    // 5. 用户消费排行
    std::cout << "\n5. 用户消费排行（前10名）：" << std::endl;
    auto user_ranking = getUserRanking(db, 10);
    if (user_ranking.empty()) {
        std::cout << "暂无用户消费数据" << std::endl;
    } else {
        for (size_t i = 0; i < user_ranking.size(); i++) {
            std::cout << "第 " << (i + 1) << " 名: "
                     << user_ranking[i].first << " - ￥"
                     << std::fixed << std::setprecision(2) << user_ranking[i].second 
                     << std::endl;
        }
    }
    
    // 6. 商品热度排行
    std::cout << "\n6. 商品热度排行（前10名）：" << std::endl;
    auto product_ranking = getProductRanking(db, 10);
    if (product_ranking.empty()) {
        std::cout << "暂无商品销售数据" << std::endl;
    } else {
        for (size_t i = 0; i < product_ranking.size(); i++) {
            std::cout << "第 " << (i + 1) << " 名: "
                     << product_ranking[i].first << " - "
                     << product_ranking[i].second << " 笔订单" << std::endl;
        }
    }

    // 在试订单状态更新之前，添加以下��码来创建个试订单
    {
        Order test_order;
        test_order.order_id = "TEST_ORDER";
        test_order.user_id = "U1";
        test_order.product_id = "P1";
        test_order.amount = 100.0;
        test_order.status = "pending";  // 从 pending 状态开始
        
        auto now = std::chrono::system_clock::now();
        auto tt = std::chrono::system_clock::to_time_t(now);
        std::stringstream ss;
        ss << std::put_time(std::localtime(&tt), "%Y-%m-%d %H:%M:%S");
        test_order.create_time = ss.str();
        
        // 写入测试订单
        db->Put(leveldb::WriteOptions(), "order:TEST_ORDER", test_order.serialize());
        createSecondaryIndex(db, "index:status", test_order.order_id, test_order.status);
        
        std::cout << "\n创建测试订单 TEST_ORDER，初始状态为 pending" << std::endl;
    }
    
    // 然后使用 TEST_ORDER 来测试状态更新
    std::cout << "\n7. 测试订单状态更新：" << std::endl;
    {
        std::string test_order_id = "TEST_ORDER";  // 使用新创建的测试订单
        std::string value;
        leveldb::Status status = db->Get(leveldb::ReadOptions(), "order:" + test_order_id, &value);
        
        if (status.ok()) {
            Order order = Order::deserialize(value);
            std::cout << "当前订单状态: " << order.status << std::endl;
            
            // 根据当前状态选择合法的下一个状态
            std::string new_status;
            if (order.status == "pending") {
                new_status = "paid";
            } else if (order.status == "paid") {
                new_status = "shipped";
            } else if (order.status == "shipped") {
                new_status = "completed";
            } else {
                std::cout << "当前订单状态 (" << order.status << ") 无法更新" << std::endl;
                new_status = "";
            }
            
            if (!new_status.empty()) {
                std::string error_msg;
                if (updateOrderStatus(db, test_order_id, new_status, error_msg)) {
                    std::cout << "订单状态更新成功: " << order.status << " -> " << new_status << std::endl;
                } else {
                    std::cout << "订单状态更新失败: " << error_msg << std::endl;
                }
            }
        } else {
            std::cout << "未找到订单: " << test_order_id << std::endl;
        }
    }

    // 在main函数中添加批量更新测试
    std::cout << "\n8. 测试批量订单状态更新：" << std::endl;
    {
        // 创建多个测试订单
        std::vector<std::string> test_order_ids;
        for (int i = 1; i <= 3; i++) {
            Order test_order;
            test_order.order_id = "BATCH_TEST_" + std::to_string(i);
            test_order.user_id = "U1";
            test_order.product_id = "P1";
            test_order.amount = 100.0 * i;
            test_order.status = "pending";
            
            auto now = std::chrono::system_clock::now();
            auto tt = std::chrono::system_clock::to_time_t(now);
            std::stringstream ss;
            ss << std::put_time(std::localtime(&tt), "%Y-%m-%d %H:%M:%S");
            test_order.create_time = ss.str();
            
            db->Put(leveldb::WriteOptions(), "order:" + test_order.order_id, test_order.serialize());
            createSecondaryIndex(db, "index:status", test_order.order_id, test_order.status);
            test_order_ids.push_back(test_order.order_id);
        }
        
        // 测试批量更新
        std::cout << "批量更新 " << test_order_ids.size() << " 个订单到 paid 状态" << std::endl;
        auto result = batchUpdateOrderStatus(db, test_order_ids, "paid", "ADMIN");
        
        std::cout << "更新成功: " << result.success_orders.size() << " 个订单" << std::endl;
        for (const auto& order_id : result.success_orders) {
            std::cout << "- " << order_id << std::endl;
            
            // 显示订单状态历史
            std::cout << "状态历史:" << std::endl;
            auto history = getOrderStatusHistory(db, order_id);
            for (const auto& record : history) {
                std::cout << "  " << record.change_time 
                         << ": " << record.old_status 
                         << " -> " << record.new_status
                         << " (by " << record.operator_id << ")" << std::endl;
            }
        }
        
        if (!result.failed_orders.empty()) {
            std::cout << "\n更新失败: " << result.failed_orders.size() << " 个订单" << std::endl;
            for (const auto& [order_id, error] : result.failed_orders) {
                std::cout << "- " << order_id << ": " << error << std::endl;
            }
        }
    }

    // 在 main 函数中添加退款功能测试
    std::cout << "\n9. 测试退款功能：" << std::endl;
    {
        // 创建一个已支付的订单用于测试退款
        Order test_order;
        test_order.order_id = "REFUND_TEST_ORDER";
        test_order.user_id = "U1";
        test_order.product_id = "P1";
        test_order.amount = 100.0;
        test_order.status = "paid";
        
        auto now = std::chrono::system_clock::now();
        auto tt = std::chrono::system_clock::to_time_t(now);
        std::stringstream ss;
        ss << std::put_time(std::localtime(&tt), "%Y-%m-%d %H:%M:%S");
        test_order.create_time = ss.str();
        test_order.payment_time = ss.str();
        
        // 写入测试订单
        db->Put(leveldb::WriteOptions(), "order:" + test_order.order_id, test_order.serialize());
        createSecondaryIndex(db, "index:status", test_order.order_id, test_order.status);
        
        std::cout << "创建测试订单: " << test_order.order_id << std::endl;
        
        // 测试创建退款申请
        std::string error_msg;
        bool success = RefundManager::createRefundRequest(
            db, test_order.order_id, 50.0, "商品质量问题", error_msg);
        
        if (success) {
            std::cout << "退款申请创建成功" << std::endl;
            
            // 查询订单的退款申请
            auto refunds = RefundManager::queryRefundsByOrder(db, test_order.order_id);
            std::cout << "订单退款申请数: " << refunds.size() << std::endl;
            
            // 处理退款申请
            if (!refunds.empty()) {
                std::cout << "\n处理退款申请: " << refunds[0].refund_id << std::endl;
                if (RefundManager::processRefundRequest(
                    db, refunds[0].refund_id, true, "ADMIN", error_msg)) {
                    std::cout << "退款申请处理成功" << std::endl;
                    
                    // 显示订单最新状态
                    std::string value;
                    if (db->Get(leveldb::ReadOptions(), 
                              "order:" + test_order.order_id, &value).ok()) {
                        Order updated_order = Order::deserialize(value);
                        std::cout << "订单最新状态: " << updated_order.status << std::endl;
                    }
                } else {
                    std::cout << "退款申请处理失败: " << error_msg << std::endl;
                }
            }
        } else {
            std::cout << "退款申请创建失败: " << error_msg << std::endl;
        }
    }

    // 添加缓存测试
    testCache(db);

    // 清理资源
    delete db;
    delete options.filter_policy;
    delete options.block_cache;

    return 0;
} 