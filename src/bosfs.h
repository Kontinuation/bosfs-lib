/**
 * bosfs - A fuse-based file system implemented on Baidu Object Storage(BOS)
 *
 * Copyright (c) 2016 Baidu.com, Inc. All rights reserved.
 *
 * @file    bosfs.h
 * @author  Song Shuangyang(songshuangyang@baidu.com)
 * @date    2016.8
 **/
#ifndef BAIDU_BOS_BOSFS_BOSFS_H
#define BAIDU_BOS_BOSFS_BOSFS_H

//#define FUSE_USE_VERSION 26

#include <stdio.h>       // for standard IO
#include <unistd.h>      // for low level IO
#include <stdint.h>      // for defined int type
#include <time.h>        // for time
#include <errno.h>       // for errno
#include <sys/time.h>    // for precise time
#include <sys/stat.h>    // for stat
#include <sys/statvfs.h> // for statvfs
#include <sys/types.h>   // for linux kernel system types

#include <fuse.h>        // fuse kernel mod

#include "common.h"
#include "util.h"
#include "bosfs_util.h"
#include "data_cache.h"

#ifndef ENOATTR
#define ENOATTR          ENODATA
#endif

BEGIN_FS_NAMESPACE

// This class declare the whole file system interface for fuse mod
class Bosfs {
public:
    static void * init(struct fuse_conn_info *conn);
    static void destroy(void *);
    static int access(const char *path, int mask);
    static int create(const char *path, mode_t mode, struct fuse_file_info *fi);
    static int open(const char *path, struct fuse_file_info *fi);
    static int read(const char *p, char *buf, size_t len, off_t offset, struct fuse_file_info *fi);
    static int write(const char *p, const char *, size_t len, off_t of, struct fuse_file_info *fi);
    static int statfs(const char *path, struct statvfs *stbuf);
    static int flush(const char *path, struct fuse_file_info *fi);
    static int fsync(const char *path, int data_sync, struct fuse_file_info *fi);
    static int release(const char *path, struct fuse_file_info *fi);

    static int symlink(const char *from, const char *to);
    static int link(const char *from, const char *to);
    static int unlink(const char *path);
    static int readlink(const char *path, char *buf, size_t size);

    static int mknod(const char *path, mode_t mode, dev_t rdev);
    static int mkdir(const char *path, mode_t mode);
    static int rmdir(const char *path);
    static int rename(const char *from, const char *to);
    static int opendir(const char *path, struct fuse_file_info *fi);
    static int readdir(const char *path, void *buf, fuse_fill_dir_t filter, off_t offset,
            struct fuse_file_info *fi);
    static int releasedir(const char* path, struct fuse_file_info *fi);

    static int chmod(const char *path, mode_t mode);
    static int chown(const char *path, uid_t uid, gid_t gid);
    static int utimens(const char *path, const struct timespec ts[2]);
    static int truncate(const char* path, off_t size);

    static int getattr(const char *path, struct stat *stbuf);
    static int listxattr(const char *path, char *list, size_t size);
    static int removexattr(const char *path, const char *name);
#if defined(__APPLE__)
    static int setxattr(const char *path, const char *name, const char *value,
            size_t size, int flags, uint32_t pos);
    static int getxattr(const char *path, const char *name, char *value, size_t size, uint32_t pos);
#else
    static int setxattr(const char *p, const char *name, const char *value, size_t size, int flags);
    static int getxattr(const char *path, const char *name, char *value, size_t size);
#endif

};


END_FS_NAMESPACE

#endif
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
