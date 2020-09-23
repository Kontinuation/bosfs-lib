/**
 * bosfs - A fuse-based file system implemented on Baidu Object Storage(BOS)
 *
 * Copyright (c) 2016 Baidu.com, Inc. All rights reserved.
 *
 * @file    bosfs_util.h
 * @author  Song Shuangyang(songshuangyang@baidu.com)
 * @date    2016.9
 **/
#ifndef BAIDU_BOS_BOSFS_BOSFS_UTIL_H
#define BAIDU_BOS_BOSFS_BOSFS_UTIL_H

#include <stdint.h>
#include <string.h>
#include <time.h>

#include <string>
#include <map>

#include "common.h"
#include "util.h"
#include "data_cache.h"
#include "bcesdk/bos/client.h"

BEGIN_FS_NAMESPACE

using baidu::bos::cppsdk::ObjectMetaData;

class DataCacheEntity;

enum StatConstraint {
    ST_BLKSIZE = 4096,
    ST_BLOCKSIZE = 512,
    ST_MINBLOCKS = 8
};

class FileManager;
class DataCache;

class BosfsUtil {
public:
    BosfsUtil();
    ~BosfsUtil();

    struct fuse_context *fuse_get_context();

    void set_file_manager(FileManager *file_manager);
    void set_data_cache(DataCache *data_cache);

    const BosfsOptions &options() {
        return _bosfs_options;
    }

    BosfsOptions &mutable_options() {
        return _bosfs_options;
    }

    SharedPtr<baidu::bos::cppsdk::Client> bos_client();
    int init_bos(BosfsOptions &bosfs_options, std::string &errmsg);

    std::string object_to_path(const std::string &object);
    std::string object_to_basename(const std::string &object, const std::string &prefix);
    std::string get_real_path(const char*);

    int check_object_access(const char *path, int mask, struct stat *pstbuf);
    int get_object_attribute(const std::string &path, struct stat *pstbuf,
            ObjectMetaData *pmeta = NULL);
    int check_path_accessible(const char *path);
    int check_parent_object_access(const char *path, int mask);
    int check_object_owner(const char *path, struct stat *pstbuf);
    DataCacheEntity *get_local_entity(const char *path, bool is_load=false);

    int head_object(const std::string &object, ObjectMetaData *meta, bool *is_dir_obj,
            bool *is_prefix);
    int multiple_head_object(std::vector<std::string> &objects,
            std::vector<struct stat *> &stats);

    int list_subitems(const std::string &prefix, int max_keys, 
            std::vector<std::string> *items) {
        std::string marker;
        return list_objects(prefix, max_keys, marker, "/", items);
    }
    // if common_prefix vector not given, then common prefixes will put into items vector
    int list_objects(const std::string &prefix, int max_keys, std::string &marker,
            const char *delimiter, std::vector<std::string> *items,
            std::vector<std::string> *common_prefix = NULL);

    void create_meta(const std::string &object_name, mode_t mode, uid_t uid, gid_t gid,
            ObjectMetaData *meta);
    int create_object(const char *path, mode_t mode, uid_t uid, gid_t gid);
    int create_object(const char *path, mode_t mode, uid_t uid, gid_t gid,
            const std::string &data);
    int delete_object(const std::string &object, std::string *version=NULL);

    int rename_file(const std::string &path, const std::string &dst, int64_t size_hint = -1);
    int rename_directory(const std::string &src, const std::string &dst);

    int change_object_meta(const std::string &object, ObjectMetaData &meta);

    void init_default_stat(struct stat *pst);

protected:
    int _parse_bosfs_options(BosfsOptions &bosfs_options, std::string &errmsg);
    int create_bos_client(std::string &errmsg);
    int exist_bucket(std::string &errmsg);
    int create_bucket(std::string &errmsg);

    int check_bucket_access();

private:
    BosfsOptions _bosfs_options;
    SharedPtr<baidu::bos::cppsdk::Client> _bos_client;
    pthread_mutex_t _client_mutex;
    FileManager *_file_manager;
    DataCache *_data_cache;
};

END_FS_NAMESPACE

#endif
