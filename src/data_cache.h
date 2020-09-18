/**
 * bosfs - A fuse-based file system implemented on Baidu Object Storage(BOS)
 *
 * Copyright (c) 2016 Baidu.com, Inc. All rights reserved.
 *
 * @file    data_cache.h
 * @author  Song Shuangyang(songshuangyang@baidu.com)
 * @date    2016.9
 **/
#ifndef BAIDU_BOS_BOSFS_DATA_CACHE_H
#define BAIDU_BOS_BOSFS_DATA_CACHE_H

#include <stdint.h>       // for standard int type
#include <sys/types.h>    // for system types(off_t...)
#include <sys/statvfs.h>  // for statvfs/fstatvfs
#include <sys/stat.h>     // for struct stat
#include <stdio.h>        // for standard io of FILE

#include <string>
#include <vector>
#include <map>
#include <list>

#include <pthread.h>

#include "common.h"
#include "util.h"
#include "bcesdk/bos/client.h"

#if defined(P_tmpdir)
#define TEMP_FILE_DIR P_tmpdir
#else
#define TEMP_FILE_DIR "/tmp"
#endif

BEGIN_FS_NAMESPACE

using baidu::bos::cppsdk::ObjectMetaData;
class DataCacheEntity;
class StatCacheFile;
class BosfsUtil;
class FileManager;

/**
 * Object block page information
 */
class ObjectPage {
public:
    ObjectPage(off_t start=0, size_t size=0, bool is_loaded=false) :
        _offset(start), _bytes(size), _loaded(is_loaded)
    {
        // nothing to do
    }

    off_t next() const
    {
        return _offset + _bytes;
    }

    off_t end() const
    {
        return 0 < _bytes ? _offset + _bytes - 1 : 0;
    }

    off_t get_offset() const
    {
        return _offset;
    }

    void set_offset(off_t s)
    {
        _offset = s;
    }

    off_t get_bytes() const
    {
        return _bytes;
    }

    void set_bytes(off_t s)
    {
        _bytes = s;
    }

    bool get_loaded() const
    {
        return _loaded;
    }

    void set_loaded(bool s)
    {
        _loaded = s;
    }

private:
    off_t  _offset;
    size_t _bytes;
    bool   _loaded;
};

/**
 * Manage the object for loading, modifying and area
 */
class ObjectPageList {
public:
    friend class DataCacheEntity;
    typedef std::list<ObjectPage *> self_type;

    static void free_list(self_type &list);

    explicit ObjectPageList(size_t size=0, bool loaded=false);
    ~ObjectPageList();
    bool init(size_t size, bool loaded);
    size_t get_size() const;
    bool resize(size_t size, bool loaded);
    bool is_page_loaded(off_t start=0, size_t size=0) const;
    bool set_page_loaded_status(off_t start, size_t size, bool loaded=true, bool compress=true);
    bool find_unloaded_pate(off_t start, off_t &ret_start, size_t &ret_size) const;
    size_t get_total_unloaded_page_size(off_t start=0, size_t size=0) const;
    int get_unloaded_pages(self_type &unloaded_list, off_t start=0, size_t size=0) const;
    bool serialize(StatCacheFile &file, bool is_output);
    void dump();

private:
    void clear();
    bool compress();
    bool parse(off_t new_pos);

private:
    self_type  _pages;
};

class StatCacheFile {
public:
    explicit StatCacheFile(DataCache *data_cache, const char *path=NULL);
    ~StatCacheFile();

    bool open_file();
    bool release();
    bool set_path(const char *path, bool is_open=true);
    int get_fd() const
    {
        return _fd;
    }

private:

private:
    DataCache *_data_cache;
    std::string _path;
    int         _fd;
};

class AutoLock {
public:
    explicit AutoLock(pthread_mutex_t *mutex) : _mutex(mutex)
    {
        pthread_mutex_lock(_mutex);
    }

    ~AutoLock()
    {
        pthread_mutex_unlock(_mutex);
    }

private:
    pthread_mutex_t *_mutex;
};

class DataCacheEntity {
public:
    typedef std::vector<std::string> etag_list_t;

    DataCacheEntity(
        BosfsUtil *bosfs_util, DataCache *data_cache, FileManager *file_manager,
        const char *tpath=NULL, const char *cpath=NULL);
    ~DataCacheEntity();

    int close_file();
    int open_file(ObjectMetaData *pmeta=NULL, ssize_t size=-1, time_t time=-1);
    bool open_and_load_all(ObjectMetaData *pmeta=NULL, size_t *size=NULL, bool force_load=false);
    int dup_file();
    bool is_open() const
    {
        return -1 != _fd;
    }
    bool is_tmpfile() const {
        return _is_tmpfile;
    }
    const char *get_path() const
    {
        return _path.c_str();
    }
    void set_path(const std::string &newpath)
    {
        _path = newpath;
    }
    void set_modified(bool is_modified) {
        _is_modified = is_modified;
    }
    int get_fd() const
    {
        return _fd;
    }
    bool is_safe_disk_space(size_t size);

    bool get_stats(struct stat &st);
    int set_mtime(time_t time);
    bool update_mtime();
    bool get_size(size_t &size);
    bool set_mode(mode_t mode);
    bool set_uid(uid_t uid);
    bool set_gid(gid_t gid);
    bool set_content_type(const char *path);
    void set_xattr(const std::string &xattr);

    int load(off_t start=0, size_t size=0);

    int row_flush(const char *tpath, bool force_sync=false);
    int flush(bool force_sync=false);
    ssize_t read(char *bytes, off_t start, size_t size, bool force_sync=false);
    ssize_t write(const char *bytes, off_t start, size_t size);

    int truncate(off_t size);

private:
    static int fill_file(int fd, unsigned char byte, size_t size, off_t start);
    void clear();
    int open_mirror_file();
    bool set_all_status(bool is_loaded);
    bool set_all_status_unloaded()
    {
        return set_all_status(false);
    }

private:
    BosfsUtil          *_bosfs_util;
    DataCache          *_data_cache;
    FileManager        *_file_manager;
    pthread_mutex_t    _entity_lock;
    ObjectPageList     _page_list;
    int                _ref_count;
    std::string        _path;             // remote object path
    std::string        _cache_path;       // local cache file path
    std::string        _mirror_path;      // mirror file path to local cache file
    int                _fd;               // cache file fd
    bool               _is_modified;      // status for file changed
    ObjectMetaData     _origin_meta;      // original meta headers
    size_t             _origin_meta_size; // original file size in original headers

    std::string       _upload_id;         // multipart upload_id when no disk space
    etag_list_t       _etaglist;          // multipart upload etags when no disk space
    off_t             _mp_start;
    size_t            _mp_size;

    // indicate that the local cache file is opened by tmpfile and will be removed on close
    bool _is_tmpfile;
    std::string _tmp_filename;
};

class DataCache {
public:
    typedef std::map<std::string, DataCacheEntity*> DataCacheMap;

    DataCache(BosfsUtil *bosfs_util, FileManager *file_manager);
    ~DataCache();

    size_t get_ensure_free_disk_space() {
        return _free_disk_space;
    }

    size_t set_ensure_free_disk_space(size_t size);

    size_t init_ensure_free_disk_space() {
        return set_ensure_free_disk_space(0);
    }

    inline bool is_cache_dir() const {
        return !_cache_dir.empty();
    }
    const char *get_cache_dir() const {
        return _cache_dir.c_str();
    }
    void set_tmp_dir(const std::string &dir) {
        _tmp_dir = dir;
    }
    const std::string &tmp_dir() const {
        return _tmp_dir;
    }
    const char *tmp_dir_cstr() const {
        return _tmp_dir.c_str();
    }

    int set_cache_dir(const std::string &dir);
    bool delete_cache_dir();
    int delete_cache_file(const char *path);
    bool make_cache_path(const char *path, std::string &cache_path,
            bool is_create_dir=true, bool is_mirror_path=false);
    bool check_cache_top_dir();

    DataCacheEntity *get_cache(const char *path);

    DataCacheEntity * open_cache(const char *path, ObjectMetaData *pmeta=NULL, ssize_t size=-1,
            time_t time=-1, bool force_tmpfile=false, bool is_create=true);

    DataCacheEntity * exist_open(const char *path);

    bool close_cache(DataCacheEntity *ent);

    // some helper functions
    bool delete_file(const char * path);
    bool check_top_dir();
    bool delete_dir();
    bool make_path(const char *path, std::string &file_path, bool is_create_dir=true);

private:
    BosfsUtil *_bosfs_util;
    FileManager *_file_manager;
    pthread_mutex_t _data_cache_lock;
    DataCacheMap _data_cache;
    std::string _cache_dir;
    std::string _tmp_dir;
    size_t _free_disk_space;
};

END_FS_NAMESPACE

#endif
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
