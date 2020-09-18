/***************************************************************************
 * 
 * Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
/**
 * @file src/file_manager.h
 * @author lili19(com@baidu.com)
 * @date 2017/09/12 09:25:48
 * @brief 
 *  
 **/
#ifndef BAIDU_BOS_BOSFS_SRC_FILE_MANAGER_H
#define BAIDU_BOS_BOSFS_SRC_FILE_MANAGER_H

#include "common.h"
#include "util.h"
#include "bcesdk/bos/client.h"
#include "bcesdk/util/lock.h"

BEGIN_FS_NAMESPACE

class BosfsUtil;

class File {
public:
    File(BosfsUtil *bosfs_util, const std::string &name)
        : _bosfs_util(bosfs_util), _name(name), _is_dir_obj(false), _is_prefix(false),
          _hit_time_s(0), _hit_bit(0) {
        pthread_mutex_init(&_mutex, NULL);
        _load_time_s = get_system_time_s();
        hit(_load_time_s);
    }
    ~File() {
        pthread_mutex_destroy(&_mutex);
    }

    const std::string &name() const { return _name; }

    void set_is_dir_obj(bool is_dir_obj) { _is_dir_obj = is_dir_obj; }
    bool is_dir_obj() const { return _is_dir_obj; }

    void set_is_prefix(bool is_prefix) { _is_prefix = is_prefix; }
    bool is_prefix() const { return _is_prefix; }

    bcesdk_ns::ObjectMetaData &meta() { return _meta; }
    const bcesdk_ns::ObjectMetaData &meta() const { return _meta; }

    int64_t load_time_s() const { return _load_time_s; }

    pthread_mutex_t &mutex() { return _mutex; }

    int64_t hit_time_s() const { return _hit_time_s; }

    void hit(int64_t now) {
        MutexGuard lock(&_mutex);
        int n = now % 64;
        if (now - _hit_time_s >= 64) {
            _hit_bit = 0;
        } else {
            int h = _hit_time_s % 64;
            uint64_t h_mask = (1UL << h);
            h_mask = (h_mask - 1) | h_mask;
            uint64_t n_mask = -1UL ^ ((1UL << n) - 1);
            _hit_bit &= h > n ? h_mask & n_mask : h_mask | n_mask;
        }
        _hit_bit |= 1UL << n;
        _hit_time_s = now;
    }
    int hit_count() const {
        uint64_t bits = _hit_bit;
        int count = 0;
        while (bits != 0) {
            ++count;
            bits &= bits - 1;
        }
        return count;
    }

    int load_meta_from_bos();
    int stat(struct stat *st);

private:
    BosfsUtil *_bosfs_util;
    std::string _name;
    bool _is_dir_obj;
    bool _is_prefix;
    bcesdk_ns::ObjectMetaData _meta;

    int64_t _load_time_s;
    pthread_mutex_t _mutex;

    int64_t _hit_time_s;
    uint64_t _hit_bit;
};

typedef SharedPtr<File> FilePtr;
typedef std::map<std::string, FilePtr> FileTable;

class FileManager {
public:
    static bool compare_cache_priority(const FilePtr &a, const FilePtr &b) {
        int c1 = a->hit_count();
        int c2 = b->hit_count();
        if (c1 == c2) {
            return a->hit_time_s() < b->hit_time_s();
        }
        return c1 > c2;
    }

public:
    FileManager(BosfsUtil *bosfs_util)
        : _bosfs_util(bosfs_util), _expire_s(-1), _cache_capacity(-1) {
    }
    ~FileManager() {
    }
    void set_expire_s(int seconds) { _expire_s = seconds; }
    void set_cache_capacity(int cap) { _cache_capacity = cap; }

    int get(const std::string &name, FilePtr *file);

    bool try_get(const std::string &name, FilePtr *file);
    void set(const std::string &name, FilePtr &file);
    void del(const std::string &name);

    void gc();

private:
    BosfsUtil *_bosfs_util;
    bcesdk_ns::TLSLock _lock;
    int _expire_s;

    FileTable _table;
    int _cache_capacity;
};

END_FS_NAMESPACE

#endif

