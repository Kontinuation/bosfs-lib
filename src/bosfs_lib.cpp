/**
 * bosfs - A fuse-based file system implemented on Baidu Object Storage(BOS)
 *
 * Copyright (c) 2020 Baidu.com, Inc. All rights reserved.
 *
 * @file    bosfs_lib.cpp
 * @author  pengbo09@baidu.com
 * @date    2020.07
 **/

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
#include "bosfs_impl.h"
#include "common.h"
#include "util.h"
#include "bosfs_util.h"
#include "sys_util.h"
#include "data_cache.h"
#include "file_manager.h"

#include <cstdio>
#include <cstdarg>

#include <sys/stat.h>
#include <dirent.h>
#include <limits.h>
#include <sys/xattr.h>

BEGIN_FS_NAMESPACE

static bool validate_mountpoint_attr(struct stat &ps, const BosfsOptions &bosfs_options) {
    BOSFS_INFO("PROC(uid=%u, gid=%u, mode=%04o) - Mountpoint(uid=%u, gid=%u, mode=%04o)",
            static_cast<unsigned int>(bosfs_options.mount_uid), static_cast<unsigned int>(bosfs_options.mount_gid),
            bosfs_options.mount_mode,
            static_cast<unsigned int>(ps.st_uid), static_cast<unsigned int>(ps.st_gid),
            static_cast<unsigned int>(ps.st_mode));
    if (0 == bosfs_options.mount_uid || ps.st_uid == bosfs_options.mount_uid) {
        return true;
    }

    if (ps.st_gid == bosfs_options.mount_gid || 1 == SysUtil::is_uid_in_group(bosfs_options.mount_uid, ps.st_gid)) {
        if (S_IRWXG == (ps.st_mode & S_IRWXG)) {
            return true;
        }
    }

    if (S_IRWXO == (ps.st_mode & S_IRWXO)) {
        return true;
    }
    return false;
}

Bosfs::Bosfs()
    : _bosfs_impl(new BosfsImpl()) {
}

Bosfs::~Bosfs() {
    delete _bosfs_impl;
    _bosfs_impl = nullptr;
}

DataCache *Bosfs::data_cache() {
    return _bosfs_impl->data_cache();
}

FileManager *Bosfs::file_manager() {
    return _bosfs_impl->file_manager();
}

int Bosfs::init_bos(BosfsOptions &bosfs_options, std::string &errmsg) {
    return _bosfs_impl->init_bos(bosfs_options, errmsg);
}

void Bosfs::init(struct fuse_conn_info *conn, fuse_config *cfg) {
    _bosfs_impl->init(conn, cfg);
}

void Bosfs::destroy() {
    _bosfs_impl->destroy();
}

int Bosfs::access(const char *path, int mask) {
    return _bosfs_impl->access(path, mask);
}

int Bosfs::create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    return _bosfs_impl->create(path, mode, fi);
}

int Bosfs::open(const char *path, struct fuse_file_info *fi) {
    return _bosfs_impl->open(path, fi);
}

int Bosfs::read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    return _bosfs_impl->read(path, buf, size, offset, fi);
}

int Bosfs::write(const char *path, const char *buf, size_t size,
    off_t offset, struct fuse_file_info *fi) {
    return _bosfs_impl->write(path, buf, size, offset, fi);
}

int Bosfs::flush(const char *path, struct fuse_file_info *fi) {
    return _bosfs_impl->flush(path, fi);
}

int Bosfs::fsync(const char *path, int isdatasync, struct fuse_file_info *fi) {
    return _bosfs_impl->fsync(path, isdatasync, fi);
}

int Bosfs::release(const char *path, struct fuse_file_info *fi) {
    return _bosfs_impl->release(path, fi);
}

int Bosfs::statfs(const char *path, struct statvfs *stbuf) {
    return _bosfs_impl->statfs(path, stbuf);
}

int Bosfs::symlink(const char *target, const char *path) {
    return _bosfs_impl->symlink(target, path);
}

int Bosfs::link(const char *from, const char *to) {//not implemented
    return _bosfs_impl->link(from, to);
}

int Bosfs::unlink(const char *path) {
    return _bosfs_impl->unlink(path);
}

int Bosfs::readlink(const char *path, char *buf, size_t size) {
    return _bosfs_impl->readlink(path, buf, size);
}

int Bosfs::mknod(const char *path, mode_t mode, dev_t rdev) {
    return _bosfs_impl->mknod(path, mode, rdev);
}

int Bosfs::mkdir(const char *path, mode_t mode) {
    return _bosfs_impl->mkdir(path, mode);
}

int Bosfs::rmdir(const char *path) {
    return _bosfs_impl->rmdir(path);
}

int Bosfs::rename(const char *from, const char *to, unsigned int flags) {
    return _bosfs_impl->rename(from, to, flags);
}

int Bosfs::opendir(const char *path, struct fuse_file_info *fi) {
    return _bosfs_impl->opendir(path, fi);
}

int Bosfs::readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
    struct fuse_file_info *fi, enum fuse_readdir_flags flags) {
    return _bosfs_impl->readdir(path, buf, filler, offset, fi, flags);
}

int Bosfs::releasedir(const char* path, struct fuse_file_info *fi) {
    return _bosfs_impl->releasedir(path, fi);
}

int Bosfs::chmod(const char *path, mode_t mode, fuse_file_info *fi) {
    return _bosfs_impl->chmod(path, mode, fi);
}

int Bosfs::chown(const char *path, uid_t uid, gid_t gid, fuse_file_info *fi) {
    return _bosfs_impl->chown(path, uid, gid, fi);
}

int Bosfs::utimens(const char *path, const struct timespec ts[2], fuse_file_info *fi) {
    return _bosfs_impl->utimens(path, ts, fi);
}

int Bosfs::getattr(const char *path, struct stat *stbuf, fuse_file_info *fi) {
    return _bosfs_impl->getattr(path, stbuf, fi);
}

int Bosfs::truncate(const char* path, off_t size, fuse_file_info *fi) {
    return _bosfs_impl->truncate(path, size, fi);
}

int Bosfs::listxattr(const char *path, char *buffer, size_t size) {
    return _bosfs_impl->listxattr(path, buffer, size);
}

int Bosfs::removexattr(const char *path, const char *name) {
    return _bosfs_impl->removexattr(path, name);
}

int Bosfs::setxattr(const char *path, const char *name, const char *value, size_t size, int flag) {
    return _bosfs_impl->setxattr(path, name, value, size, flag);
}

int Bosfs::getxattr(const char *path, const char *name, char *value, size_t size) {
    return _bosfs_impl->getxattr(path, name, value, size);
}

// fuse operation handlers for bosfs 
static inline Bosfs *get_bosfs() {
    return reinterpret_cast<Bosfs *>(fuse_get_context()->private_data);
}

static void *bosfs_init(struct fuse_conn_info *conn, fuse_config *cfg) {
    Bosfs *bosfs = get_bosfs();
    bosfs->init(conn, cfg);
    return bosfs;
}

static void bosfs_destroy(void *arg) {
    Bosfs *bosfs = reinterpret_cast<Bosfs *>(arg);
    bosfs->destroy();
}

static int bosfs_access(const char *path, int mask) {
    return get_bosfs()->access(path, mask);
}

static int bosfs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    return get_bosfs()->create(path, mode, fi);
}

static int bosfs_open(const char *path, struct fuse_file_info *fi) {
    return get_bosfs()->open(path, fi);
}

static int bosfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    return get_bosfs()->read(path, buf, size, offset, fi);
}

static int bosfs_write(const char *path, const char *buf, size_t size,
    off_t offset, struct fuse_file_info *fi) {
    return get_bosfs()->write(path, buf, size, offset, fi);
}

static int bosfs_flush(const char *path, struct fuse_file_info *fi) {
    return get_bosfs()->flush(path, fi);
}

static int bosfs_fsync(const char *path, int isdatasync, struct fuse_file_info *fi) {
    return get_bosfs()->fsync(path, isdatasync, fi);
}

static int bosfs_release(const char *path, struct fuse_file_info *fi) {
    return get_bosfs()->release(path, fi);
}

static int bosfs_statfs(const char *path, struct statvfs *stbuf) {
    return get_bosfs()->statfs(path, stbuf);
}

static int bosfs_symlink(const char *target, const char *path) {
    return get_bosfs()->symlink(target, path);
}

static int bosfs_link(const char *from, const char *to) {//not implemented
    return get_bosfs()->link(from, to);
}

static int bosfs_unlink(const char *path) {
    return get_bosfs()->unlink(path);
}

static int bosfs_readlink(const char *path, char *buf, size_t size) {
    return get_bosfs()->readlink(path, buf, size);
}

static int bosfs_mknod(const char *path, mode_t mode, dev_t rdev) {
    return get_bosfs()->mknod(path, mode, rdev);
}

static int bosfs_mkdir(const char *path, mode_t mode) {
    return get_bosfs()->mkdir(path, mode);
}

static int bosfs_rmdir(const char *path) {
    return get_bosfs()->rmdir(path);
}

static int bosfs_rename(const char *from, const char *to, unsigned int flags) {
    return get_bosfs()->rename(from, to, flags);
}

static int bosfs_opendir(const char *path, struct fuse_file_info *fi) {
    return get_bosfs()->opendir(path, fi);
}

static int bosfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
    struct fuse_file_info *fi, enum fuse_readdir_flags flags) {
    return get_bosfs()->readdir(path, buf, filler, offset, fi, flags);
}

static int bosfs_releasedir(const char* path, struct fuse_file_info *fi) {
    return get_bosfs()->releasedir(path, fi);
}

static int bosfs_chmod(const char *path, mode_t mode, fuse_file_info *fi) {
    return get_bosfs()->chmod(path, mode, fi);
}

static int bosfs_chown(const char *path, uid_t uid, gid_t gid, fuse_file_info *fi) {
    return get_bosfs()->chown(path, uid, gid, fi);
}

static int bosfs_utimens(const char *path, const struct timespec ts[2], fuse_file_info *fi) {
    return get_bosfs()->utimens(path, ts, fi);
}

static int bosfs_getattr(const char *path, struct stat *stbuf, fuse_file_info *fi) {
    return get_bosfs()->getattr(path, stbuf, fi);
}

static int bosfs_truncate(const char* path, off_t size, fuse_file_info *fi) {
    return get_bosfs()->truncate(path, size, fi);
}

static int bosfs_listxattr(const char *path, char *buffer, size_t size) {
    return get_bosfs()->listxattr(path, buffer, size);
}

static int bosfs_removexattr(const char *path, const char *name) {
    return get_bosfs()->removexattr(path, name);
}

static int bosfs_setxattr(const char *path, const char *name, const char *value, size_t size, int flag) {
    return get_bosfs()->setxattr(path, name, value, size, flag);
}

static int bosfs_getxattr(const char *path, const char *name, char *value, size_t size) {
    return get_bosfs()->getxattr(path, name, value, size);
}

int bosfs_prepare_fs_operations(
    const std::string &bucket_path, const std::string &mountpoint,
    Bosfs *bosfs, BosfsOptions &bosfs_options,
    struct fuse_operations &bosfs_operation, std::string &errmsg) {

    // Resolve bucket path to bucket name and bucket prefix
    std::size_t pos = bucket_path.find("/");
    bosfs_options.bucket = bucket_path.substr(0, pos);   
    if (pos != std::string::npos) {
        bosfs_options.bucket_prefix = bucket_path.substr(pos+1);
    }

    // Resolve mountpoint
    char mountpoint_buffer[10240];
    if (realpath(mountpoint.c_str(), mountpoint_buffer) == NULL) {
        return return_with_error_msg(
            errmsg, "unable get absolute path of mountpoint:%s, errno:%d", mountpoint.c_str(), errno);
    }

    struct stat stbuf;
    if (-1 == stat(mountpoint_buffer, &stbuf)) {
        return return_with_error_msg(errmsg, "unable to access MOUNTPOINT %s: %s", mountpoint_buffer, strerror(errno));
    }
    if (!(S_ISDIR(stbuf.st_mode))) {
        return return_with_error_msg(errmsg, "MOUNTPOINT: %s is not a directory", mountpoint_buffer);
    }

    if (bosfs->init_bos(bosfs_options, errmsg) != 0) {
        return 3;
    }

    if (!validate_mountpoint_attr(stbuf, bosfs_options)) {
        return return_with_error_msg(errmsg, "MOUNTPOINT: %s permission denied", mountpoint_buffer);
    }

    // Link the file system interface to fuse
    memset(&bosfs_operation, 0, sizeof(struct fuse_operations));
    bosfs_operation.init        = bosfs_init;
    bosfs_operation.destroy     = bosfs_destroy;
    bosfs_operation.access      = bosfs_access;
    bosfs_operation.create      = bosfs_create;
    bosfs_operation.open        = bosfs_open;
    bosfs_operation.read        = bosfs_read;
    bosfs_operation.write       = bosfs_write;
    bosfs_operation.statfs      = bosfs_statfs;
    bosfs_operation.flush       = bosfs_flush;
    bosfs_operation.fsync       = bosfs_fsync;
    bosfs_operation.release     = bosfs_release;
    bosfs_operation.symlink     = bosfs_symlink;
    bosfs_operation.link        = bosfs_link;
    bosfs_operation.unlink      = bosfs_unlink;
    bosfs_operation.readlink    = bosfs_readlink;
    bosfs_operation.mknod       = bosfs_mknod;
    bosfs_operation.mkdir       = bosfs_mkdir;
    bosfs_operation.rmdir       = bosfs_rmdir;
    bosfs_operation.rename      = bosfs_rename;
    bosfs_operation.opendir     = bosfs_opendir;
    bosfs_operation.readdir     = bosfs_readdir;
    bosfs_operation.releasedir  = bosfs_releasedir;
    bosfs_operation.chmod       = bosfs_chmod;
    bosfs_operation.chown       = bosfs_chown;
    bosfs_operation.utimens     = bosfs_utimens;
    bosfs_operation.truncate    = bosfs_truncate;
    bosfs_operation.getattr     = bosfs_getattr;
    bosfs_operation.listxattr   = bosfs_listxattr;
    bosfs_operation.getxattr    = bosfs_getxattr;
    bosfs_operation.setxattr    = bosfs_setxattr;
    bosfs_operation.removexattr = bosfs_removexattr;
    return 0;
}

END_FS_NAMESPACE
