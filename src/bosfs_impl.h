/**
 * bosfs - A fuse-based file system implemented on Baidu Object Storage(BOS)
 *
 * Copyright (c) 2020 Baidu.com, Inc. All rights reserved.
 *
 * @file    bosfs_impl.h
 * @author  pengbo09@baidu.com
 * @date    2020.09
 **/
#ifndef BAIDU_BOS_BOSFS_BOSFS_IMPL_H
#define BAIDU_BOS_BOSFS_BOSFS_IMPL_H

#include <stdio.h>       // for standard IO
#include <unistd.h>      // for low level IO
#include <stdint.h>      // for defined int type
#include <time.h>        // for time
#include <errno.h>       // for errno
#include <sys/time.h>    // for precise time
#include <sys/stat.h>    // for stat
#include <sys/statvfs.h> // for statvfs
#include <sys/types.h>   // for linux kernel system types

#include "bosfs_lib/bosfs_lib.h"
#include "bosfs_util.h"
#include "sys_util.h"
#include "data_cache.h"
#include "file_manager.h"

BEGIN_FS_NAMESPACE

class BosfsImpl {
public:
    BosfsImpl();
    ~BosfsImpl();

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
    int readdir(const char *path, void *buf, fuse_fill_dir_t filter, off_t offset,
        struct fuse_file_info *fi, enum fuse_readdir_flags flags);
    int releasedir(const char* path, struct fuse_file_info *fi);

    int chmod(const char *path, mode_t mode, fuse_file_info *fi);
    int chown(const char *path, uid_t uid, gid_t gid, fuse_file_info *fi);
    int utimens(const char *path, const struct timespec ts[2], fuse_file_info *fi);
    int truncate(const char* path, off_t size, fuse_file_info *fi);

    int getattr(const char *path, struct stat *stbuf, fuse_file_info *fi);
    int listxattr(const char *path, char *list, size_t size);
    int removexattr(const char *path, const char *name);
    int setxattr(const char *p, const char *name, const char *value, size_t size, int flags);
    int getxattr(const char *path, const char *name, char *value, size_t size);

private:
    BosfsUtil _bosfs_util;
    FileManager _file_manager;
    DataCache _data_cache;
};

END_FS_NAMESPACE

#endif // BAIDU_BOS_BOSFS_BOSFS_IMPL_H
