// leveldb_example.cpp
// 这是一个详细的LevelDB使用案例，展示了如何打开数据库、插入和检索数据，以及关闭数据库。

#include <iostream>
#include <cassert>
#include <string>
#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "leveldb/comparator.h"
#include "leveldb/filter_policy.h"
#include "leveldb/cache.h"

// 自定义比较器
class ReverseComparator : public leveldb::Comparator {
public:
    int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const override {
        return -a.compare(b); // 反向比较
    }
    const char* Name() const override {
        return "reverse_comparator";
    }
    void FindShortestSeparator(std::string* start, const leveldb::Slice& limit) const override {}
    void FindShortSuccessor(std::string* key) const override {}
};

int main() {
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true; // 如果数据库不存在，则创建它

    // 使用自定义比较器
    ReverseComparator reverse_comparator;
    options.comparator = &reverse_comparator;

    // 使用过滤策略
    options.filter_policy = leveldb::NewBloomFilterPolicy(10);

    // 启用压缩
    options.compression = leveldb::kSnappyCompression;

    // 设置缓存大小
    options.block_cache = leveldb::NewLRUCache(100 * 1048576); // 100MB cache

    // 打开数据库
    leveldb::Status status = leveldb::DB::Open(options, "example_db", &db);
    assert(status.ok()); // 确保数据库打开成功

    // 插入数据
    std::string key1 = "key1";
    std::string value1 = "value1";
    status = db->Put(leveldb::WriteOptions(), key1, value1);
    assert(status.ok()); // 确保数据插入成功

    // 检索数据
    std::string value;
    status = db->Get(leveldb::ReadOptions(), key1, &value);
    assert(status.ok()); // 确保数据检索成功
    std::cout << "Retrieved value: " << value << " for key: " << key1 << std::endl;

    // 批量写操作
    leveldb::WriteBatch batch;
    batch.Put("key2", "value2");
    batch.Put("key3", "value3");
    batch.Delete("key1");
    status = db->Write(leveldb::WriteOptions(), &batch);
    assert(status.ok()); // 确保批量写操作成功

    // 迭代数据
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::cout << "Key: " << it->key().ToString() << ", Value: " << it->value().ToString() << std::endl;
    }
    assert(it->status().ok()); // 确保迭代器状态正常
    delete it;

    // 删除数据
    status = db->Delete(leveldb::WriteOptions(), "key2");
    assert(status.ok()); // 确保数据删除成功

    // 使用快照
    leveldb::ReadOptions read_options;
    read_options.snapshot = db->GetSnapshot();
    status = db->Get(read_options, "key3", &value);
    assert(status.ok()); // 确保通过快照检索数据成功
    std::cout << "Retrieved value from snapshot: " << value << " for key: key3" << std::endl;
    db->ReleaseSnapshot(read_options.snapshot);

    // 处理错误
    status = db->Get(leveldb::ReadOptions(), "non_existent_key", &value);
    if (!status.ok()) {
        std::cerr << "Error retrieving non_existent_key: " << status.ToString() << std::endl;
    }

    // 关闭数据库
    delete db;

    // 清理过滤策略和缓存
    delete options.filter_policy;
    delete options.block_cache;

    return 0;
}
