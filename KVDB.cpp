#include <sys/stat.h>
#include <stdexcept>

#include "fcntl.h"
#include "unistd.h"
#include "KVDB.h"

KVDBHandler::KVDBHandler(const std::string &db_file) {
    this->_fd = ::open(db_file.c_str(), O_APPEND | O_CREAT | O_RDWR, S_IRWXU);
    if (0 > this->_fd) {
//        printf(
//                "Failed to open DB file. [file='%s' res=%d]",
//                db_file.c_str(),
//                this->_fd);
        exit(KVDB_INVALID_AOFPATH);
    }
    file_path = db_file;
    off_t end;
    end = lseek(_fd, 0, SEEK_END);
    lseek(_fd, 0, SEEK_SET);
    while (end != lseek(_fd, 0, SEEK_CUR)) {
        int len_key, len_value;
        time_t time;
        time = 0;
        size_t ret;
        ret = 0;
        std::string key, value;
        off_t offset;
        offset = lseek(_fd, 0, SEEK_CUR);
        ret += read(_fd, &time, sizeof(time));
        ret += read(_fd, &len_key, sizeof(len_key));
        try {
            key.resize(len_key);
        } catch (const std::bad_alloc &e) {
//        printf("Failed to malloc. [size=%ld]\n", end);
            exit(KVDB_MALLOC_ERROR);
        } catch (const std::length_error &e) {
//        printf("Wrong length. [size=%ld]\n", end);
            exit(KVDB_MALLOC_ERROR);
        }
        ret += read(_fd, (char *) key.data(), len_key);
        ret += read(_fd, &len_value, sizeof(len_value));
        if (len_value == -1) continue;
        try {
            value.resize(len_value);
        } catch (const std::bad_alloc &e) {
//        printf("Failed to malloc. [size=%ld]\n", end);
            exit(KVDB_MALLOC_ERROR);
        } catch (const std::length_error &e) {
//        printf("Wrong length. [size=%ld]\n", end);
            exit(KVDB_MALLOC_ERROR);
        }
        ret += read(_fd, (char *) value.data(), len_value);
        if (ret != len_value + len_key + sizeof(len_key) + sizeof(len_value) + sizeof(time)) {
            exit(KVDB_IO_ERROR);
        }
        index[key] = offset;
        if (time != 0) {
            minheap.push({key, time});
        }
    }
}

std::string KVDBHandler::get_path() {
    return file_path;
}

KVDBHandler::~KVDBHandler() {
    if (0 <= this->_fd) {
        close(this->_fd);
    }
}

int KVDBHandler::set(const std::string &key, const std::string value, const time_t time) {
    int len_key, len_value;
    size_t ret;
    ret = 0;
    len_key = key.length();
    len_value = value.length();
    if (0 == len_key) {
//        printf(
//                "Invalid key."
//        );
        return KVDB_INVALID_KEY;
    }
    const int &file = _fd;
    if (0 > lseek(file, 0, SEEK_END)) {
//        printf(
//                "Failed to seek file. [fd=%d]", file
//        );
        return KVDB_FAILED_SEEKFILE;
    }
    off_t offset;
    offset = lseek(file, 0, SEEK_CUR);
    ret += write(file, &time, sizeof(time));
    ret += write(file, &len_key, sizeof(len_key));
    ret += write(file, key.c_str(), len_key);
    ret += write(file, &len_value, sizeof(len_value));
    ret += write(file, value.c_str(), len_value);
    size_t size;
    size = sizeof(len_key) + len_key + sizeof(len_value) + len_value + sizeof(time);
    if (ret != size) {
//        printf(
//                "Failed to write file. [fd=%d write_size=%ld ret_size=%ld]\n",
//                file,
//                size,
//                ret
//        );
        return KVDB_IO_ERROR;
    }
    index[key] = offset;
    if (time != 0) {
        minheap.push({key, time});
    }
    return KVDB_OK;
}

int KVDBHandler::del(const std::string &key) {
    int len_key, len_value;
    time_t time;
    size_t ret;
    ret = 0;
    len_key = key.length();
    len_value = -1;
    if (0 == len_key) {
//        printf(
//                "Invalid key."
//        );
        return KVDB_INVALID_KEY;
    }
    const int &file = _fd;
    std::string value;
    if (get(key, value, time) != KVDB_OK) {
        return KVDB_INVALID_KEY;
    }
    if (0 > lseek(file, 0, SEEK_END)) {
//        printf(
//                "Failed to seek file. [fd=%d]", file
//        );
        return KVDB_FAILED_SEEKFILE;
    }
    ret += write(file, &time, sizeof(time));
    ret += write(file, &len_key, sizeof(len_key));
    ret += write(file, key.c_str(), len_key);
    ret += write(file, &len_value, sizeof(len_value));
    size_t size;
    size = sizeof(len_key) + len_key + sizeof(len_value) + sizeof(time);
    if (ret != size) {
//        printf(
//                "Failed to write file. [fd=%d write_size=%ld ret_size=%ld]\n",
//                file,
//                size,
//                ret
//        );
        return KVDB_IO_ERROR;
    }
    index.erase(key);
    return KVDB_OK;
}


int KVDBHandler::get(const std::string &key, std::string &value, time_t time) {
    time_t cur;
    cur = ::time(NULL);
    while (!minheap.empty() && cur > minheap.top().time) {
        expire_del(minheap.top().key, minheap.top().time);
        minheap.pop();
    }
    int len_key, len_value;
    size_t ret;
    ret = 0;
    len_key = key.length();
    if (0 == len_key) {
//        printf(
//                "Invalid key."
//        );
        return KVDB_INVALID_KEY;
    }
    try {
        index.at(key);
    } catch (const std::out_of_range &e) {
        return KVDB_INVALID_KEY;
    }
    off_t offset;
    std::string key2;
    offset = index.at(key);
    lseek(_fd, offset, SEEK_SET);
    ret += read(_fd, &time, sizeof(time));
    ret += read(_fd, &len_key, sizeof(len_key));
    try {
        key2.resize((len_key));
    } catch (const std::bad_alloc &e) {
//        printf("Failed to malloc. [size=%ld]\n", end);
        return KVDB_MALLOC_ERROR;
    } catch (const std::length_error &e) {
//        printf("Wrong length. [size=%ld]\n", end);
        return KVDB_MALLOC_ERROR;
    }
    ret += read(_fd, (char *) key2.data(), len_key);
    ret += read(_fd, &len_value, sizeof(len_value));
    try {
        value.resize((len_value));
    } catch (const std::bad_alloc &e) {
//        printf("Failed to malloc. [size=%ld]\n", end);
        return KVDB_MALLOC_ERROR;
    } catch (const std::length_error &e) {
//        printf("Wrong length. [size=%ld]\n", end);
        return KVDB_MALLOC_ERROR;
    }
    ret += read(_fd, (char *) value.data(), len_value);
    if (ret != len_value + len_key + sizeof(len_key) + sizeof(len_value) + sizeof(time)) {
        return KVDB_IO_ERROR;
    }
    return KVDB_OK;
}

int KVDBHandler::purge() {
    std::string tmp_path = "temp";
    int tmp_file;
    tmp_file = ::open(tmp_path.c_str(), O_TRUNC | O_CREAT | O_RDWR, S_IRWXU);
    if (0 > tmp_file) {
//        printf(
//                "Failed to open DB file. [file='%s' res=%d]",
//                tmp_path.c_str(),
//                tmp_file);
        return KVDB_NO_SPACE_LEFT_ON_DEVICES;
    }
    if (0 < tmp_file) {
        close(tmp_file);
    }
    KVDBHandler tmp_kv(tmp_path);
    const int &file = _fd;
    off_t end;
    end = lseek(file, 0, SEEK_END);
    if (0 > end) {
//        printf(
//                "Failed to seek file. [fd=%d]\n", file
//        );
        return KVDB_IO_ERROR;
    }
    std::string value;
    time_t time;
    for (auto &i: index) {
        if (get(i.first, value, time) == KVDB_OK) {
            tmp_kv.set(i.first, value, time);
        }
    }
    std::string old_path = get_path();
    if (0 < _fd) {
        close(_fd);
    }
    remove(old_path.c_str());
    if (0 < tmp_file) {
        close(tmp_file);
    }
    rename(tmp_path.c_str(), old_path.c_str());
    this->_fd = ::open(old_path.c_str(), O_APPEND | O_CREAT | O_RDWR, S_IRWXU);
    if (0 > this->_fd) {
//        printf(
//                "Failed to open DB file. [file='%s' res=%d]",
//                db_file.c_str(),
//                this->_fd);
        return KVDB_INVALID_AOFPATH;
    }
    file_path = old_path;
    index = tmp_kv.index;
    return KVDB_OK;
}

int KVDBHandler::expires(const std::string &key, int n) {
    time_t time;
    int len_key;
    len_key = key.length();
    if (0 == len_key) {
//        printf(
//                "Invalid key."
//        );
        return KVDB_INVALID_KEY;
    }
    std::string value;

    if (get(key, value, time) != KVDB_OK) {
        return KVDB_INVALID_KEY;
    }
    time = ::time(NULL) + n;
    set(key, value, time);
    return KVDB_OK;
}

int KVDBHandler::expire_del(const std::string key, const time_t time){
    int len_key, len_value;
    const int &file = _fd;
    std::string value;
    size_t ret;
    time_t _time;
    ret = 0;
    len_key = key.length();
    if (0 == len_key) {
//        printf(
//                "Invalid key."
//        );
        return KVDB_INVALID_KEY;
    }
    try {
        index.at(key);
    } catch (const std::out_of_range &e) {
        return KVDB_INVALID_KEY;
    }
    off_t offset;
    std::string key2;
    offset = index.at(key);
    lseek(_fd, offset, SEEK_SET);
    ret += read(_fd, &_time, sizeof(_time));
    if (ret != sizeof(_time)) {
        return KVDB_IO_ERROR;
    }
    if (_time == time) {
        len_value = -1;
        ret = 0;
        if (0 > lseek(file, 0, SEEK_END)) {
//        printf(
//                "Failed to seek file. [fd=%d]", file
//        );
            return KVDB_FAILED_SEEKFILE;
        }
        time_t time2;
        time2 = 0;
        offset = lseek(file, 0, SEEK_CUR);
        ret += write(file, &time2, sizeof(time2));
        ret += write(file, &len_key, sizeof(len_key));
        ret += write(file, key.c_str(), len_key);
        ret += write(file, &len_value, sizeof(len_value));
        index[key] = offset;
        size_t size;
        size = sizeof(len_key) + len_key + sizeof(len_value) + sizeof(time2);
        if (ret != size) {
//        printf(
//                "Failed to write file. [fd=%d write_size=%ld ret_size=%ld]\n",
//                file,
//                size,
//                ret
//        );
            return KVDB_IO_ERROR
        }
    }
    return KVDB_OK;
}
