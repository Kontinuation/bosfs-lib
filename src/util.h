/**
 * bosfs - A fuse-based file system implemented on Baidu Object Storage(BOS)
 *
 * Copyright (c) 2016 Baidu.com, Inc. All rights reserved.
 *
 * @file    util.h
 * @author  Song Shuangyang(songshuangyang@baidu.com)
 * @date    2016.8
 **/
#ifndef BAIDU_BOS_BOSFS_UTIL_H
#define BAIDU_BOS_BOSFS_UTIL_H

#include "common.h"
#include "bcesdk/util/util.h"
#include <sys/time.h>
#include <pthread.h>

#define BOSFS_FATAL(fmt, ...) LOGF(FATAL, fmt, ##__VA_ARGS__)
#define BOSFS_ERR(fmt, ...) LOGF(ERROR, fmt, ##__VA_ARGS__)
#define BOSFS_WARN(fmt, ...) LOGF(WARN, fmt, ##__VA_ARGS__)
#define BOSFS_INFO(fmt, ...) LOGF(INFO, fmt, ##__VA_ARGS__)
#define BOSFS_DEBUG(fmt, ...) LOGF(DEBUG, fmt, ##__VA_ARGS__)

BEGIN_FS_NAMESPACE

using bcesdk_ns::StringUtil;

// Facility of return code for BOSFS
enum ret_code_t {
    BOSFS_OK = 0,
    BOSFS_BOS_CLIENT_UNINITIALIZED = 1000,
    BOSFS_BOS_CLIENT_REQUEST_ERROR,
    BOSFS_BOS_SERVICE_ERROR,
    BOSFS_CREATE_BUCKET_FAILED,
    BOSFS_BUCKET_NOT_EXISTS,
    BOSFS_BUCKET_ACCESS_DENIED,
    BOSFS_AK_SK_INVALID,
    BOSFS_HOST_INVALID,
    BOSFS_TIMEOUT_INVALID,
    BOSFS_MEMORY_ERROR,
    BOSFS_ADD_META_CACHE_FAIL,
    BOSFS_CONVERT_HEADER_TO_META_CACHE_FAIL,
    BOSFS_LIST_OBJECTS_FAIL,
    BOSFS_BUCKET_NOT_EMPTY,
    BOSFS_OBJECT_KEY_INVALID,
    BOSFS_NOT_ALLOWED_OPERATION,
    BOSFS_OBJECT_NOT_EXIST,
    BOSFS_NOT_DIRECTORY
};
const char * stringfy_ret_code(int code);

template<typename T>
class ScopedPtr {
public:
    ScopedPtr() {
        _p = new T();
    }
    ScopedPtr(T *p) {
        _p = p;
    }
    ~ScopedPtr() {
        if (_p != NULL) {
            delete _p;
            _p = NULL;
        }
    }

    T *get() {
        return _p;
    }
    T *release() {
        T *p = _p;
        _p = NULL;
        return p;
    }

    inline T &operator *() { return *_p; }
    inline T *operator -> () { return _p; }
private:
    T *_p;
};

template<typename T>
class SmartArray {
public:
    SmartArray(int size) {
        _p = new T[size];
    }
    SmartArray(T *p) {
        _p = p;
    }
    ~SmartArray() {
        if (_p != NULL) {
            delete[] _p;
            _p = NULL;
        }
    }

    T *get() {
        return _p;
    }
    T *release() {
        T *p = _p;
        _p = NULL;
        return p;
    }

    inline T &operator [](int i) { return _p[i]; }
    inline T &operator *() { return *_p; }
    inline T *operator -> () { return _p; }
private:
    T *_p;
};

class Ref {
public:
    Ref() : _cnt(1) {
        pthread_mutex_init(&_lock, NULL);
    }
    ~Ref() {
        pthread_mutex_destroy(&_lock);
    }

    // return before
    int add() {
        pthread_mutex_lock(&_lock);
        int tmp = _cnt++;
        pthread_mutex_unlock(&_lock);
        return tmp;
    }

    // return remains
    int dec() {
        pthread_mutex_lock(&_lock);
        int tmp = --_cnt;
        pthread_mutex_unlock(&_lock);
        return tmp;
    }

    int count() {
        pthread_mutex_lock(&_lock);
        int tmp = _cnt;
        pthread_mutex_unlock(&_lock);
        return tmp;
    }

private:
    pthread_mutex_t _lock;
    int _cnt;
};

template<typename T>
class SharedPtr {
public:
    SharedPtr() {
        _p = NULL;
        _ref = NULL;
    }
    SharedPtr(T *p) {
        _p = p;
        _ref = new Ref();
    }
    SharedPtr(const SharedPtr &ptr) {
        _p = ptr._p;
        _ref = ptr._ref;
        if (_ref != NULL) {
            _ref->add();
        }
    }
    ~SharedPtr() {
        reset();
    }

    SharedPtr &operator=(const SharedPtr &ptr) {
        reset();
        _p = ptr._p;
        _ref = ptr._ref;
        if (_ref != NULL) {
            _ref->add();
        }
        return *this;
    }

    T *get() { return _p; }
    const T *get() const { return _p; }

    void reset() {
        if (_ref != NULL && _ref->dec() == 0) {
            delete _ref;
            if (_p != NULL) {
                delete _p;
            }
        }
    }
    void reset(T *p) {
        reset();
        _p = p;
        _ref = new Ref();
    }

    int refcount() {
        return _ref->count();
    }

    inline const T &operator *() const { return *_p; }
    inline const T *operator -> () const { return _p; }

    inline T &operator *() { return *_p; }
    inline T *operator -> () { return _p; }
private:
    T *_p;
    Ref *_ref;
};

class MutexGuard {
public:
    MutexGuard(pthread_mutex_t *lock) {
        _lock = lock;
        if (_lock != NULL) {
            pthread_mutex_lock(_lock);
        }
    }
    ~MutexGuard() {
        unlock();
    }

    void unlock() {
        if (_lock != NULL) {
            pthread_mutex_unlock(_lock);
            _lock = NULL;
        }
    }
private:
    MutexGuard(const MutexGuard &);
    MutexGuard &operator=(const MutexGuard &);
    pthread_mutex_t *_lock;
};

inline int64_t get_system_time_s() {
    struct timeval t;
    gettimeofday(&t, NULL);
    return t.tv_sec;
}

END_FS_NAMESPACE

#endif

