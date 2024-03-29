#pragma once

#include<string>
#include<unordered_map>
#include<queue>
#include<vector>
#include<time.h>

const int maxsize = 10;
const int KVDB_OK = 0;
const int KVDB_INVALID_AOFPATH = 1;
const int KVDB_INVALID_KEY = 2;
const int KVDB_NO_SPACE_LEFT_ON_DEVICES = 3;
const int KVDB_FAILED_SEEKFILE = 4;
const int KVDB_IO_ERROR = 5;
const int KVDB_MALLOC_ERROR = 6;

struct LRU_Node {
    std::string value;
    LRU_Node *prior = NULL;
    LRU_Node *next = NULL;
};

struct tmp {
    std::string key;
    time_t time;
};

struct tmp2 {
    bool operator()(tmp &a, tmp &b) {
        return a.time > b.time;
    }
};

struct type {
    std::string key;
    std::string value;
};


class LRU {
private:
    std::unordered_map<std::string, LRU_Node *> cache;
    LRU_Node *head = NULL;
    LRU_Node *tail = NULL;
    int volume;
    int count = 0;
public:
    LRU(int volume = 10);

    void set(const std::string &key, const std::string &value);

    bool get(const std::string &key, std::string &value);

    bool del(const std::string &key);

};


class KVDBHandler {
public:
    KVDBHandler(const std::string &db_file);

    ~KVDBHandler();

    int set(const std::string &key, const std::string value, const time_t time = 0);

    int get(const std::string &key, std::string &value, time_t time);

    int del(const std::string &key);

    int purge();

    int expires(const std::string &key, int n);

    int expire_del(const std::string key, const time_t time);

    std::string get_path();

private:
    int _fd;
    std::unordered_map<std::string, off_t> index;
    std::string file_path;
    std::priority_queue<tmp, std::vector<tmp>, tmp2> minheap;
    LRU cache;
};

