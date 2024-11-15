#include <iostream>
#include <cassert>
#include <string>
#include "leveldb/db.h"

int main() {
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;

    // 打开数据库
    leveldb::Status status = leveldb::DB::Open(options, "./testdb", &db);
    if (!status.ok()) {
        std::cerr << "无法打开/创建数据库: " << status.ToString() << std::endl;
        return 1;
    }

    // 写入一些键值对
    status = db->Put(leveldb::WriteOptions(), "key1", "值1");
    assert(status.ok());
    status = db->Put(leveldb::WriteOptions(), "key2", "值2");
    assert(status.ok());
    status = db->Put(leveldb::WriteOptions(), "key3", "值3");
    assert(status.ok());

    // 读取并显示值
    std::string value;
    status = db->Get(leveldb::ReadOptions(), "key1", &value);
    if (status.ok()) {
        std::cout << "key1 的值是: " << value << std::endl;
    }

    // 使用迭代器遍历所有键值对
    std::cout << "\n遍历所有键值对：" << std::endl;
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::cout << "键: " << it->key().ToString() 
                 << ", 值: " << it->value().ToString() << std::endl;
    }
    delete it;

    // 删除一个键
    std::cout << "\n删除 key1" << std::endl;
    status = db->Delete(leveldb::WriteOptions(), "key1");
    assert(status.ok());

    // 再次遍历确认删除结果
    std::cout << "\n删除后的键值对：" << std::endl;
    it = db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::cout << "键: " << it->key().ToString() 
                 << ", 值: " << it->value().ToString() << std::endl;
    }
    delete it;

    // 关闭数据库
    delete db;

    std::cout << "\n示例运行完成！" << std::endl;
    return 0;
} 