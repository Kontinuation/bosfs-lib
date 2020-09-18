/**
 * bosfs - A fuse-based file system implemented on Baidu Object Storage(BOS)
 *
 * Copyright (c) 2020 Baidu.com, Inc. All rights reserved.
 *
 * @file    bosfs_lib.h
 * @author  pengbo09@baidu.com
 * @date    2020.07
 **/
#ifndef BAIDU_BOS_BOSFS_BOSFS_LIB_H
#define BAIDU_BOS_BOSFS_BOSFS_LIB_H

#include <string>
#include <map>
#ifdef INCLUDE_FUSE3
#include <fuse3/fuse.h>
#else
#include <fuse.h>
#endif

#ifndef ENOATTR
#define ENOATTR          ENODATA
#endif

namespace baidu {
namespace bos {
namespace bosfs {

struct BosfsOptions {
    // Common global variable
    std::string        endpoint;
    std::string        bucket;
    std::string        bucket_prefix;
    std::string        ak;
    std::string        sk;
    std::string        sts_token;
    std::string        storage_class;

    // cache and file manager configs
    std::string        cache_dir;
    int                meta_expires_s = 0;
    int                meta_capacity = -1;
    std::string        tmp_dir;

    // multipart upload options
    int64_t            multipart_size = 10 * 1024 * 1024;
    int                multipart_parallel = 10;
    int64_t            multipart_threshold = 100 * 1024 * 1024;

    // Variables for command arguments
    time_t             mount_time = 0;
    uid_t              mount_uid = 0;
    gid_t              mount_gid = 0;
    mode_t             mount_mode = 0;
    mode_t             mount_umask = 0022;
    bool               is_mount_umask = false;
    bool               allow_other = false;
    uid_t              bosfs_uid = 0;
    gid_t              bosfs_gid = 0;
    mode_t             bosfs_mask = 0;
    bool               is_bosfs_uid = false;
    bool               is_bosfs_gid = false;
    bool               is_bosfs_umask = false;
    bool               remove_cache = false;
    bool               create_bucket = false;
    uint64_t           bos_client_timeout = 1200;
};

class DataCache;
class FileManager;
class BosfsImpl;

// This class declare the whole file system interface for fuse mod
class Bosfs {
public:
    Bosfs();
    ~Bosfs();

    int init_bos(BosfsOptions &bosfs_options, std::string &errmsg);

    DataCache *data_cache();
    FileManager *file_manager();

    void init(struct fuse_conn_info *conn, fuse_config *cfg);
    void destroy();
    int access(const char *path, int mask);
    int create(const char *path, mode_t mode, struct fuse_file_info *fi);
    int open(const char *path, struct fuse_file_info *fi);
    int read(const char *p, char *buf, size_t len, off_t offset, struct fuse_file_info *fi);
    int write(const char *p, const char *, size_t len, off_t of, struct fuse_file_info *fi);
    int statfs(const char *path, struct statvfs *stbuf);
    int flush(const char *path, struct fuse_file_info *fi);
    int fsync(const char *path, int data_sync, struct fuse_file_info *fi);
    int release(const char *path, struct fuse_file_info *fi);

    int symlink(const char *from, const char *to);
    int link(const char *from, const char *to);
    int unlink(const char *path);
    int readlink(const char *path, char *buf, size_t size);

    int mknod(const char *path, mode_t mode, dev_t rdev);
    int mkdir(const char *path, mode_t mode);
    int rmdir(const char *path);
    int rename(const char *from, const char *to, unsigned int flags);
    int opendir(const char *path, struct fuse_file_info *fi);
    int readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
        struct fuse_file_info *fi, enum fuse_readdir_flags flags);
    int releasedir(const char* path, struct fuse_file_info *fi);

    int chmod(const char *path, mode_t mode);
    int chown(const char *path, uid_t uid, gid_t gid);
    int utimens(const char *path, const struct timespec ts[2]);
    int truncate(const char* path, off_t size);

    int getattr(const char *path, struct stat *stbuf);
    int listxattr(const char *path, char *list, size_t size);
    int removexattr(const char *path, const char *name);
    int setxattr(const char *p, const char *name, const char *value, size_t size, int flags);
    int getxattr(const char *path, const char *name, char *value, size_t size);

private:
    BosfsImpl *_bosfs_impl;
};

// interface for directly mounting bos using fuse
int bosfs_prepare_fs_operations(
    const std::string &bucket_path, const std::string &mountpoint,
    Bosfs *bosfs, BosfsOptions &bosfs_options,
    struct fuse_operations &bosfs_operation, std::string &errmsg);

} // namespace bosfs
} // namespace bos
} // namespace baidu

#endif // BAIDU_BOS_BOSFS_BOSFS_LIB_H
