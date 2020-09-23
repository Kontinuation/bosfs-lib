/**
 * bosfs - A fuse-based file system implemented on Baidu Object Storage(BOS)
 *
 * Copyright (c) 2020 Baidu.com, Inc. All rights reserved.
 *
 * @file    bosfs_impl.cpp
 * @author  pengbo09@baidu.com
 * @date    2020.09
 **/

#include "bosfs_impl.h"

#include <stdio.h>       // for standard IO
#include <unistd.h>      // for low level IO
#include <stdint.h>      // for defined int type
#include <time.h>        // for time
#include <errno.h>       // for errno
#include <sys/time.h>    // for precise time
#include <sys/stat.h>    // for stat
#include <sys/statvfs.h> // for statvfs
#include <sys/types.h>   // for linux kernel system types

#include <string>
#include <limits.h>
#include <sys/xattr.h>

BEGIN_FS_NAMESPACE

BosfsImpl::BosfsImpl()
    : _bosfs_util(),
      _file_manager(&_bosfs_util),
      _data_cache(&_bosfs_util, &_file_manager) {
    _bosfs_util.set_file_manager(&_file_manager);
    _bosfs_util.set_data_cache(&_data_cache);
}

BosfsImpl::~BosfsImpl() {
}

DataCache *BosfsImpl::data_cache() {
    return &_data_cache;
}

FileManager *BosfsImpl::file_manager() {
    return &_file_manager;
}

int BosfsImpl::init_bos(BosfsOptions &bosfs_options, std::string &errmsg) {
    return _bosfs_util.init_bos(bosfs_options, errmsg);
}

void BosfsImpl::init(struct fuse_conn_info *conn, fuse_config *cfg) {
    BOSFS_INFO("fuse init");
    cfg->use_ino = 0;
    cfg->nullpath_ok = 1;

    cfg->entry_timeout = 0;
    cfg->attr_timeout = 0;
    cfg->negative_timeout = 0;

#ifndef __APPLE__
    if (static_cast<unsigned int>(conn->capable) & FUSE_CAP_ATOMIC_O_TRUNC) {
        conn->want |= FUSE_CAP_ATOMIC_O_TRUNC;
    }
#endif
}

void BosfsImpl::destroy() {
    BOSFS_INFO("fuse destroy");
}

int BosfsImpl::access(const char *path, int mask) {
    std::string realpath = _bosfs_util.get_real_path(path);
    path = realpath.c_str();
    BOSFS_INFO("access [path=%s][mask=%s%s%s%s]", path,
            ((mask & R_OK) == R_OK) ? "R_OK" : "",
            ((mask & W_OK) == W_OK) ? "W_OK" : "",
            ((mask & X_OK) == X_OK) ? "X_OK" : "",
            (mask == F_OK) ? "F_OK" : "");
    int ret = _bosfs_util.check_object_access(path, mask, NULL);
    return ret;
}

int BosfsImpl::create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    BOSFS_INFO("create [path=%s][mode=%04o][flags=%d]", path, mode, fi->flags);
    std::string realpath = _bosfs_util.get_real_path(path);
    path = realpath.c_str();
    int ret = BOSFS_OK;
    struct fuse_context *pctx = _bosfs_util.fuse_get_context();
    if (NULL == pctx) {
        return -EIO;
    }
    ret = _bosfs_util.check_parent_object_access(path, X_OK | W_OK);
    if (ret != 0) {
        return ret;
    }

    ScopedPtr<ObjectMetaData> meta(new ObjectMetaData());
    _bosfs_util.create_meta(path, mode, pctx->uid, pctx->gid, meta.get());

    DataCacheEntity *ent = _data_cache.open_cache(path, meta.get(), 0, -1, false, true);
    if (ent == NULL) {
        return -EIO;
    }
    ent->set_modified(true);
    FilePtr file(new File(&_bosfs_util, path));
    file->meta().move_from(*meta);
    _file_manager.set(path, file);
    fi->fh = (int64_t) ent;
    return 0;
}

int BosfsImpl::open(const char *path, struct fuse_file_info *fi)
{
    BOSFS_INFO("open [path=%s][flags=%d]", path, fi->flags);
    std::string realpath = _bosfs_util.get_real_path(path);
    path = realpath.c_str();
    int ret = _bosfs_util.check_parent_object_access(path, X_OK);
    if (ret != 0) {
        return ret;
    }
    int access = 0;
    if (fi->flags & O_WRONLY) {
        access |= W_OK;
    } else if (fi->flags & O_RDWR) {
        access |= W_OK | R_OK;
    } else {
        access |= R_OK;
    }
    ret = _bosfs_util.check_object_access(path, access, NULL);
    if (0 != ret) {
        return ret;
    }
    struct stat st;
    ObjectMetaData meta;
    ret = _bosfs_util.get_object_attribute(path, &st, &meta);
    if (ret != 0) {
        return ret;
    }
    bool need_truncate = false;
    if (static_cast<unsigned int>(fi->flags) & O_TRUNC) {
        if (0 != st.st_size) {
            need_truncate = true;
        }
    }
    if (!S_ISREG(st.st_mode) || S_ISLNK(st.st_mode)) {
        st.st_mtime = -1;
    }

    DataCacheEntity *ent = _data_cache.open_cache(
            path, &meta, static_cast<ssize_t>(st.st_size), st.st_mtime, false, true);
    if (ent == NULL)  {
        _file_manager.del(path);
        return -EIO;
    }

    if (need_truncate) {
        ret = ent->truncate(0);
        if (ret != 0) {
            BOSFS_ERR("truncate file %s failed, errno: %d", path, ret);
            return ret;
        }
    }

    fi->fh = (int64_t) ent;
    return 0;
}

int BosfsImpl::read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    BOSFS_INFO("read [path=%s][size=%u][offset=%ld][fd=%lx]", path, size, offset, fi->fh);
    DataCacheEntity *ent = (DataCacheEntity *) fi->fh;
    size_t real_size = 0;
    if (!ent->get_size(real_size) || real_size <= 0) {
        BOSFS_DEBUG("%s", "file size is 0, break to read");
        return 0;
    }
    return ent->read(buf, offset, size, false);
}

int BosfsImpl::write(const char *path, const char *buf, size_t size,
        off_t offset, struct fuse_file_info *fi) {
    BOSFS_INFO("write [path=%s][size=%u][offset=%ld][fd=%lx]", path, size, offset, fi->fh);
    DataCacheEntity *ent = (DataCacheEntity *) fi->fh;
    return ent->write(buf, offset, size);
}

int BosfsImpl::flush(const char *path, struct fuse_file_info *fi) {
    BOSFS_INFO("flush [path=%s][fh=%lx]", path, fi->fh);
    DataCacheEntity *ent = (DataCacheEntity *) fi->fh;
    ent->update_mtime();
    if (ent->flush(false) != 0) {
        return -EIO;
    }
    return 0;
}

int BosfsImpl::fsync(const char *path, int isdatasync, struct fuse_file_info *fi) {
    BOSFS_INFO("fsync [path=%s][fh=%lx]", path, fi->fh);
    DataCacheEntity *ent = (DataCacheEntity *) fi->fh;
    if (!isdatasync) {
        ent->update_mtime();
    }
    if (ent->flush(false) != 0) {
        return -EIO;
    }
    return 0;
}

int BosfsImpl::release(const char *path, struct fuse_file_info *fi) {
    BOSFS_INFO("fuse RELEASE: path:%s", path);

    DataCacheEntity *ent = (DataCacheEntity *) fi->fh;
    _data_cache.close_cache(ent);
    return 0;
}

int BosfsImpl::statfs(const char *path, struct statvfs *stbuf) {
    (void) path;
    // 256TB
    stbuf->f_bsize   = 0x1000000;
    stbuf->f_blocks  = 0x1000000;
    stbuf->f_bfree   = 0x1000000;
    stbuf->f_bavail  = 0x1000000;
    stbuf->f_namemax = NAME_MAX;
    return 0;
}

int BosfsImpl::symlink(const char *target, const char *path) {
    BOSFS_INFO("symlink %s -> %s", path, target);
    std::string realpath = _bosfs_util.get_real_path(path);
    path = realpath.c_str();

    struct fuse_context *pctx = _bosfs_util.fuse_get_context();
    if (pctx == NULL) {
        return -EIO;
    }
    int ret = 0;
    if (0 != (ret = _bosfs_util.check_parent_object_access(path, W_OK | X_OK))) {
        return ret;
    }
    ret = _bosfs_util.check_object_access(path, F_OK, NULL);
    if (ret != -ENOENT) {
        if (ret == 0) {
            return -EEXIST;
        }
        return ret;
    }
    mode_t mode = S_IFLNK | S_IRWXU | S_IRWXG | S_IRWXO;
    ret = _bosfs_util.create_object(path, mode, pctx->uid, pctx->gid, target);
    if (ret != 0) {
        return -EIO;
    }
    _file_manager.del(path);
    return 0;
}

int BosfsImpl::link(const char *from, const char *to) {//not implemented
    (void) from;
    (void) to;
    return -EPERM;
}

int BosfsImpl::unlink(const char *path) {
    // search permission of path components and write permission of parent directory
    std::string realpath = _bosfs_util.get_real_path(path);
    path = realpath.c_str();
    int ret = _bosfs_util.check_parent_object_access(path, W_OK | X_OK);
    if (ret != 0) {
        return ret;
    }
    ret = _bosfs_util.delete_object(path + 1);
    _file_manager.del(path);
    if (ret != 0) {
        return ret;
    }
    _data_cache.delete_cache_file(path);
    return 0;
}

int BosfsImpl::readlink(const char *path, char *buf, size_t size) {
    if (!buf || 0 >= size) {
        return 0;
    }

    std::string realpath = _bosfs_util.get_real_path(path);
    path = realpath.c_str();

    DataCacheEntity *ent = NULL;
    if (NULL == (ent = _bosfs_util.get_local_entity(path))) {
        BOSFS_ERR("could not get entity(file = %s)", path);
        return -EIO;
    }

    size_t read_size = 0;
    ent->get_size(read_size);

    if (size <= read_size) {
        read_size = size - 1;
    }

    int ret = ent->read(buf, 0, read_size);
    _data_cache.close_cache(ent);
    if (ret < 0) {
        BOSFS_ERR("could not read file(file=%s, errno=%d)", path, ret);
        return ret;
    }
    buf[ret] = '\0';
    return 0;
}

int BosfsImpl::mknod(const char *path, mode_t mode, dev_t rdev) {
    BOSFS_INFO("mknod [path=%s][mode=%04o][dev=%ju]", path, mode, rdev);
    std::string realpath = _bosfs_util.get_real_path(path);
    path = realpath.c_str();
    struct fuse_context *pctx = NULL;
    if (NULL == (pctx = _bosfs_util.fuse_get_context())) {
        return -EIO;
    }
    int ret = _bosfs_util.check_parent_object_access(path, W_OK | X_OK);
    if (ret != 0) {
        return ret;
    }
    // check existence
    struct stat st;
    ret = _bosfs_util.get_object_attribute(path, &st);
    if (ret != -ENOENT) {
        if (ret == 0) {
            return -EEXIST;
        }
        return ret;
    }

    ret = _bosfs_util.create_object(path, mode, pctx->uid, pctx->gid);
    if (ret != 0) {
        BOSFS_ERR("could not create object for special file, result = %d", ret);
        return -EIO;
    }
    _file_manager.del(path);
    return ret;
}

int BosfsImpl::mkdir(const char *path, mode_t mode) {
    BOSFS_INFO("mkdir [path=%s][mode=%04o]", path, mode);
    std::string realpath = _bosfs_util.get_real_path(path);
    path = realpath.c_str();
    struct fuse_context *pctx = NULL;
    if (NULL == (pctx = _bosfs_util.fuse_get_context())) {
        return -EIO;
    }

    int ret = _bosfs_util.check_parent_object_access(path, W_OK | X_OK);
    if (ret != 0) {
        return ret;
    }
    // check existence
    struct stat st;
    ret = _bosfs_util.get_object_attribute(path, &st);
    if (ret != -ENOENT) {
        if (ret == 0) {
            return -EEXIST;
        }
        return ret;
    }

    //ret = _bosfs_util.create_directory_object(path, mode, pctx->uid, pctx->gid);
    ret = _bosfs_util.create_object(path, mode | S_IFDIR, pctx->uid, pctx->gid);
    if (ret != 0) {
        return -EIO;
    }
    _file_manager.del(path);
    return 0;
}

int BosfsImpl::rmdir(const char *path) {
    std::string realpath = _bosfs_util.get_real_path(path);
    path = realpath.c_str();
    std::string object_name(path + 1);
    // forbid delete mountpoint
    if (object_name.empty()) {
        return -EPERM;
    }

    int ret = _bosfs_util.check_parent_object_access(path, W_OK | X_OK);
    if (ret != 0) {
        return ret;
    }

    // Directory must be empty
    std::vector<std::string> subitems;
    if (_bosfs_util.list_subitems(object_name + "/", 2, &subitems) != 0) {
        return -EIO;
    }
    if (!subitems.empty()) {
        return -ENOTEMPTY;
    }
    ret = _bosfs_util.delete_object(object_name + "/");
    if (ret != 0) {
        if (ret == BOSFS_OBJECT_NOT_EXIST) {
            ret = _bosfs_util.delete_object(object_name);
        }
    }
    // force refresh meta info, may be delete is success but response is timeout
    _file_manager.del(path);
    if (ret != 0) {
        if (ret == BOSFS_OBJECT_NOT_EXIST) {
            return -ENOENT;
        }
        return -EIO;
    }
    return 0;
}

// rename between local and bos, fuse will convert to copy and unlink
// so this rename() call only happens in the mountpoint
int BosfsImpl::rename(const char *from, const char *to, unsigned int flags) {
    BOSFS_INFO("rename [from=%s][to=%s][flags=%u]", from, to, flags);
    if (flags) {
        // renameat2 is not supported
        return -EINVAL;
    }
    std::string realpath_from = _bosfs_util.get_real_path(from);
    from = realpath_from.c_str();
    std::string realpath_to = _bosfs_util.get_real_path(to);
    to = realpath_to.c_str();
    int ret = 0;
    // unlink file in src
    ret = _bosfs_util.check_parent_object_access(to, W_OK | X_OK);
    if (ret != 0) {
        return ret;
    }
    // unlink(when need overwrite) and create file in dst
    ret = _bosfs_util.check_parent_object_access(from, W_OK | X_OK);
    if (ret != 0) {
        return ret;
    }

    // existence of src
    struct stat st;
    ret = _bosfs_util.get_object_attribute(from, &st);
    if (ret != 0) {
        return ret;
    }

    if (S_ISDIR(st.st_mode)) { // rename directory
        ret = _bosfs_util.rename_directory(from + 1, to + 1);
    } else {
        ret = _bosfs_util.rename_file(from + 1, to + 1, st.st_size);
    }
    if (0 != ret) {
        BOSFS_ERR("rename failed, from: %s, to: %s", from, to);
        return -EIO;
    }
    return 0;
}


int BosfsImpl::opendir(const char *path, struct fuse_file_info *fi) {
    BOSFS_INFO("opendir [path=%s][flags=%d]", path, fi->flags);
    std::string realpath = _bosfs_util.get_real_path(path);
    const char *orig_path = path;
    path = realpath.c_str();
    int ret = _bosfs_util.check_path_accessible(path);
    if (ret != 0) {
        return ret;
    }
    ret = _bosfs_util.check_object_access(path, R_OK, NULL);
    if (ret == 0) {
        fi->fh = (uint64_t) strdup(orig_path);
    }
    return ret;
}

int BosfsImpl::readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
    struct fuse_file_info *fi, enum fuse_readdir_flags flags) {
    (void) flags;
    if (fi != nullptr) {
        path = (const char *) fi->fh;
    }
    enum fuse_fill_dir_flags fill_flags = (enum fuse_fill_dir_flags) 0;
    LOG(INFO) << "readdir, path:" << path << " offset:" << offset;
    std::string path_(path+1);
    std::string prefix = _bosfs_util.options().bucket_prefix + path_;
    if (!path_.empty()) {
        prefix = prefix + "/";
    }
    std::string marker;
    do {
        std::vector<std::string> items;
        std::vector<std::string> prefixes;
        int ret = _bosfs_util.list_objects(prefix, 1000, marker, "/", &items, &prefixes);
        if (ret != 0) {
            return -EIO;
        }
        struct stat default_st;
        _bosfs_util.init_default_stat(&default_st);
        for (size_t i = 0; i < prefixes.size(); ++i) {
            std::string dir_path = _bosfs_util.object_to_path(prefixes[i]);
            FilePtr file(new File(&_bosfs_util, dir_path));
            file->set_is_prefix(true);
            _file_manager.set(dir_path, file);
            std::string basename = _bosfs_util.object_to_basename(prefixes[i], prefix);
            if (filler(buf, basename.c_str(), &default_st, 0, fill_flags)) {
                break;
            }
        }
        std::vector<struct stat> stats(items.size());
        std::vector<std::string> no_cache_items;
        std::vector<struct stat *> no_cache_stats;
        for (size_t i = 0; i < items.size(); ++i) {
            FilePtr file;
            if (_file_manager.try_get(_bosfs_util.object_to_path(items[i]), &file)) {
                file->stat(&stats[i]);
            } else {
                no_cache_items.push_back(items[i]);
                no_cache_stats.push_back(&stats[i]);
            }
        }
        _bosfs_util.multiple_head_object(no_cache_items, no_cache_stats);
        for (size_t i = 0; i < items.size(); ++i) {
            std::string basename = _bosfs_util.object_to_basename(items[i], prefix);
            if (filler(buf, basename.c_str(), &stats[i], 0, fill_flags)) {
                break;
            }
        }
    } while (!marker.empty());
    return 0;
}

int BosfsImpl::releasedir(const char* path, struct fuse_file_info *fi) {
    (void) path;
    free((char *) fi->fh);
    fi->fh = 0;
    return 0;
}

int BosfsImpl::chmod(const char *path, mode_t mode, fuse_file_info *fi) {
    std::string realpath;
    if (fi != nullptr) {
        DataCacheEntity *ent = (DataCacheEntity *) fi->fh;
        path = ent->get_path();
        BOSFS_INFO("chmod [fi->fh=%lx][mode=%04o][path:%s]", fi->fh, mode, path);
    } else {
        BOSFS_INFO("chmod [path=%s][mode=%04o]", path, mode);
        realpath = _bosfs_util.get_real_path(path);
        path = realpath.c_str();
    }

    if (0 == strcmp(path, "/")) {
        LOG(ERROR) << "ignored chmod for bucket, path:" << path << " mode:" << mode;
        return 0;
    }
    int ret = _bosfs_util.check_path_accessible(path);
    if (ret != 0) {
        return ret;
    }
    struct stat st;
    ret = _bosfs_util.check_object_owner(path, &st);
    if (ret != 0) {
        return ret;
    }
    ObjectMetaData meta;
    ret = _bosfs_util.get_object_attribute(path, &st, &meta);
    if (ret != 0) {
        return ret;
    }
    std::string object_name(path + 1);
    if (S_ISDIR(st.st_mode)) {
        object_name += '/';
    }
    meta.set_user_meta("bosfs-mode", mode);
    ret = _bosfs_util.change_object_meta(object_name, meta);
    if (ret != 0) {
        if (ret == -ENOENT) {
            DataCacheEntity *ent = _data_cache.exist_open(path);
            if (ent != NULL) {
                ent->set_mode(mode);
                _data_cache.close_cache(ent);
                return 0;
            }
        }
        return ret;
    }
    _file_manager.del(path);
    return 0;
}

int BosfsImpl::chown(const char *path, uid_t uid, gid_t gid, fuse_file_info *fi) {
    std::string realpath;
    if (fi != nullptr) {
        DataCacheEntity *ent = (DataCacheEntity *) fi->fh;
        path = ent->get_path();
        BOSFS_INFO("chown [fi->fh=%lx][uid=%d][gid=%d][path:%s]", fi->fh, uid, gid, path);
    } else {
        BOSFS_INFO("chown [path=%s][uid=%d][gid=%d]", path, uid, gid);
        realpath = _bosfs_util.get_real_path(path);
        path = realpath.c_str();
    }
    if (0 == strcmp(path, "/")) {
        LOG(ERROR) << "ignored chown for bucket, path:" << path << " uid:" << uid << " gid:"
            << gid;
        return 0;
    }
    int ret = _bosfs_util.check_path_accessible(path);
    if (ret != 0) {
        return ret;
    }
    
    struct fuse_context *pctx = _bosfs_util.fuse_get_context();
    if (pctx == NULL) {
        return -EIO;
    }
    // only root can chown
    if (pctx->uid != 0) {
        return -EPERM;
    }
    struct stat st;
    ObjectMetaData meta;
    ret = _bosfs_util.get_object_attribute(path, &st, &meta);
    if (ret != 0) {
        return ret;
    }
    DataCacheEntity *ent = _data_cache.exist_open(path);
    if (ent != NULL) {
        ent->set_uid(uid);
        ent->set_gid(gid);
        _data_cache.close_cache(ent);
        return 0;
    }
    std::string object_name(path + 1);
    if (S_ISDIR(st.st_mode)) {
        object_name += '/';
    }
    meta.set_user_meta("bosfs-uid", uid);
    meta.set_user_meta("bosfs-gid", gid);
    ret = _bosfs_util.change_object_meta(object_name, meta);
    if (ret != 0) {
        return ret;
    }
    _file_manager.del(path);
    return 0;
}

int BosfsImpl::utimens(const char *path, const struct timespec ts[2], fuse_file_info *fi) {
    std::string realpath;
    if (fi != nullptr) {
        DataCacheEntity *ent = (DataCacheEntity *) fi->fh;
        path = ent->get_path();
    } else {
        realpath = _bosfs_util.get_real_path(path);
        path = realpath.c_str();
    }

    if (strcmp(path, "/") == 0) {
        _bosfs_util.mutable_options().mount_time = ts[1].tv_sec;
        return 0;
    }
    int ret = _bosfs_util.check_path_accessible(path);
    if (ret != 0) {
        return ret;
    }
    struct stat st;
    ret = _bosfs_util.check_object_access(path, W_OK, &st);
    if (ret != 0) {
        return ret;
    }
    ret = _bosfs_util.check_object_owner(path, &st);
    if (ret != 0) {
        return ret;
    }
    ObjectMetaData meta;
    ret = _bosfs_util.get_object_attribute(path, &st, &meta);
    if (ret != 0) {
        return ret;
    }
    std::string object_name(path + 1);
    if (S_ISDIR(st.st_mode)) {
        object_name += '/';
    }
    DataCacheEntity *ent = _data_cache.exist_open(path);
    if (ent != NULL) {
	ent->set_mode(strtol(meta.user_meta("bosfs-mode").c_str(), NULL, 0));
        ret = ent->set_mtime(ts[1].tv_sec);
        _data_cache.close_cache(ent);
        return ret;
    }
    meta.set_user_meta("bosfs-mtime", ts[1].tv_sec);
    ret = _bosfs_util.change_object_meta(object_name, meta);
    if (ret != 0) {
        return ret;
    }
    _file_manager.del(path);
    return 0;
}

int BosfsImpl::getattr(const char *path, struct stat *stbuf, fuse_file_info *fi) {
    // st t() requires path executable
    std::string realpath;
    if (fi != nullptr) {
        DataCacheEntity *ent = (DataCacheEntity *) fi->fh;
        path = ent->get_path();
    } else {
        realpath = _bosfs_util.get_real_path(path);
        path = realpath.c_str();
    }

    int ret = _bosfs_util.check_path_accessible(path);
    if (ret != 0) {
        return ret;
    }
    // check existence
    ret = _bosfs_util.get_object_attribute(path, stbuf);
    if (ret != 0) {
        return ret;
    }
    if (stbuf == NULL) {
        return 0;
    }
    if (!S_ISREG(stbuf->st_mode)) {
        return 0;
    }
    DataCacheEntity *ent = _data_cache.exist_open(path);
    // file is opened, may be some writes in cache not flushed, get cache filesize
    if (ent != NULL) {
        struct stat tmp;
        if (ent->get_stats(tmp)) {
            stbuf->st_size = tmp.st_size;
            stbuf->st_blksize = tmp.st_blksize;
            stbuf->st_blocks = tmp.st_blocks;
            stbuf->st_atime = tmp.st_atime;
            stbuf->st_mtime = tmp.st_mtime;
        }
        _data_cache.close_cache(ent);
    }
    return 0;
}

int BosfsImpl::truncate(const char* path, off_t size, fuse_file_info *fi) {

    std::string realpath;
    if (fi != nullptr) {
        DataCacheEntity *ent = (DataCacheEntity *) fi->fh;
        path = ent->get_path();
        BOSFS_INFO("truncate [fi->fh=%lx][size:%lu][path:%s]", fi->fh, size, path);
    } else {
        BOSFS_INFO("truncate [path=%s][size:%lu]", path, size);
        realpath = _bosfs_util.get_real_path(path);
        path = realpath.c_str();
    }

    BOSFS_INFO("truncate file %s to %ld", path, size);
    int ret = _bosfs_util.check_path_accessible(path);
    if (ret != 0) {
        return ret;
    }
    ret = _bosfs_util.check_object_access(path, W_OK, NULL);
    if (0 != ret) {
        return ret;
    }
    struct stat st;
    ObjectMetaData meta;
    ret = _bosfs_util.get_object_attribute(path, &st, &meta);
    if (ret != 0) {
        return ret;
    }
    DataCacheEntity *ent = _data_cache.open_cache(path, &meta, st.st_size, st.st_mtime);
    if (ent == NULL) {
        return -EIO;
    }
    ret = ent->truncate(size);
    if (ret != 0) {
        _data_cache.close_cache(ent);
        return ret;
    }
    ret = ent->load(0, size);
    if (ret != 0) {
        _data_cache.close_cache(ent);
        return ret;
    }
    ret = ent->flush(true);
    if (ret != 0) {
        _data_cache.close_cache(ent);
        return ret;
    }
    _data_cache.close_cache(ent);
    _file_manager.del(path);
    return 0;
}

/*
 * use to extract a key-value string in a xattr string;
 * if name was empty then match every key, or else exactly match the name.
 * @param next        will be set as postition of the name should be insert into
 * @param delim_pos   if matched then will be set as the position of ':' or npos
 * @return npos on not matched, and start pos of matched key-value string
 **/
static size_t locate_xattr(const std::string &xattr, const std::string &name, size_t *next,
    size_t *delim_pos = NULL) {
    size_t result = std::string::npos;
    size_t next_pos = 0;
    if (next != NULL) {
        next_pos = *next;
    }
    size_t pos = next_pos;
    while (pos < xattr.length()) {
        size_t end = xattr.find(';', pos);
        if (end != std::string::npos) {
            next_pos = end + 1;
        } else {
            next_pos = xattr.length();
        }
        std::string key = xattr.substr(pos, next_pos - pos);
        size_t sp = key.find(':');
        if (sp != std::string::npos) {
            key.resize(sp);
            sp += pos;
        }
        int c = 0;
        if (!name.empty()) {
            c = key.compare(0, name.length(), name);
        }
        if (c == 0) {
            result = pos;
            if (delim_pos != NULL) {
                *delim_pos = sp;
            }
            break;
        } else if (c > 0) {
            next_pos = pos;
            break;
        }
        pos = next_pos;
    }
    if (next != NULL) {
        *next = next_pos;
    }
    return result;
}

int BosfsImpl::listxattr(const char *path, char *buffer, size_t size) {
    std::string realpath = _bosfs_util.get_real_path(path);
    path = realpath.c_str();
    int ret = _bosfs_util.check_path_accessible(path);
    if (ret != 0) {
        return ret;
    }
    struct stat st;
    ObjectMetaData meta;
    // check existence
    ret = _bosfs_util.get_object_attribute(path, &st, &meta);
    if (ret != 0) {
        if (ret == -ENOENT) {
            return 0;
        }
        return ret;
    }

    size_t off = 0;
    const std::string &xattr = meta.user_meta("bosfs-xattr");

    size_t next = 0;
    size_t delim_pos = 0;
    size_t pos = locate_xattr(xattr, "", &next, &delim_pos);
    while (pos != std::string::npos) {
        std::string key;
        size_t end = next;
        if (end != 0 && xattr[end - 1] == ';') {
            --end;
        }
        if (delim_pos != std::string::npos) {
            key = xattr.substr(pos, delim_pos - pos);
        } else {
            key = xattr.substr(pos, end);
        }
        int data_len = key.length() + 1;
        if (size > 0) {
            if (size < off + data_len) {
                return -ERANGE;
            }
            memcpy(buffer + off, key.c_str(), data_len);
        }
        off += data_len;
        pos = locate_xattr(xattr, "", &next, &delim_pos);
    }
    return off;
}

int BosfsImpl::removexattr(const char *path, const char *name) {
    std::string realpath = _bosfs_util.get_real_path(path);
    path = realpath.c_str();
    if (0 == strcmp(path, "/")) {
        LOG(ERROR) << "ignored removexattr for bucket, path:" << path << " name:" << name;
        return 0;
    }
    int ret = _bosfs_util.check_path_accessible(path);
    if (ret != 0) {
        return ret;
    }
    struct stat st;
    ret = _bosfs_util.check_object_owner(path, &st);
    if (ret != 0) {
        return ret;
    }
    ObjectMetaData meta;
    ret = _bosfs_util.get_object_attribute(path, &st, &meta);
    if (ret != 0) {
        return ret;
    }
    std::string &xattr = (*meta.mutable_user_meta())["bosfs-xattr"];
    size_t end = 0;
    size_t pos = locate_xattr(xattr, name, &end);
    if (pos != std::string::npos) {
        xattr.erase(pos, end - pos);
    } else {
        return -ENOATTR;
    }
    DataCacheEntity *ent = _data_cache.exist_open(path);
    if (ent != NULL) {
        ent->set_xattr(xattr);
        _data_cache.close_cache(ent);
        return ret;
    }
    std::string object_name(path + 1);
    if (S_ISDIR(st.st_mode)) {
        object_name += '/';
    }
    ret = _bosfs_util.change_object_meta(object_name, meta);
    if (ret != 0) {
        return ret;
    }
    _file_manager.del(path);
    return 0;
}

int BosfsImpl::setxattr(const char *path, const char *name, const char *value, size_t size, int flag) {
    std::string realpath = _bosfs_util.get_real_path(path);
    path = realpath.c_str();
    if (0 == strcmp(path, "/")) {
        LOG(ERROR) << "ignored setxattr for bucket, path:" << path << " name:" << name
            << " value:" << std::string(value, size);
        return 0;
    }
    int ret = _bosfs_util.check_path_accessible(path);
    if (ret != 0) {
        return ret;
    }
    struct stat st;
    ret = _bosfs_util.check_object_owner(path, &st);
    if (ret != 0) {
        return ret;
    }
    ObjectMetaData meta;
    ret = _bosfs_util.get_object_attribute(path, &st, &meta);
    if (ret != 0) {
        return ret;
    }
    std::string val = StringUtil::base64_encode(std::string(value, size));
    std::string &xattr = (*meta.mutable_user_meta())["bosfs-xattr"];
    size_t next = 0;
    size_t delim_pos = 0;
    size_t pos = locate_xattr(xattr, name, &next, &delim_pos);
    if (pos != std::string::npos) {
        if ((flag & XATTR_CREATE) == XATTR_CREATE) {
            return -EEXIST;
        }
        if (next != 0 && xattr[next - 1] == ';') {
            --next;
        }
        if (delim_pos == std::string::npos) {
            xattr.insert(next, ":" + val);
        } else {
            xattr.replace(delim_pos + 1, next - delim_pos - 1, val);
        }
    } else {
        if ((flag & XATTR_REPLACE) == XATTR_REPLACE) {
            return -ENOATTR;
        }
        std::string record = std::string(name) + ":" + val;
        if (next != xattr.length()) {
            record += ";";
        } else if (*xattr.rbegin() != ';') {
            record = ";" + record;
        }
        xattr.insert(next, record);
    }
    DataCacheEntity *ent = _data_cache.exist_open(path);
    if (ent != NULL) {
        ent->set_xattr(xattr);
        _data_cache.close_cache(ent);
        return ret;
    }
    std::string object_name(path + 1);
    if (S_ISDIR(st.st_mode)) {
        object_name += '/';
    }
    ret = _bosfs_util.change_object_meta(object_name, meta);
    if (ret != 0) {
        return ret;
    }
    _file_manager.del(path);
    return 0;
}

int BosfsImpl::getxattr(const char *path, const char *name, char *value, size_t size) {
    std::string realpath = _bosfs_util.get_real_path(path);
    path = realpath.c_str();
    int ret = _bosfs_util.check_path_accessible(path);
    if (ret != 0) {
        return ret;
    }
    struct stat st;
    ObjectMetaData meta;
    // check existence
    ret = _bosfs_util.get_object_attribute(path, &st, &meta);
    if (ret != 0) {
        return ret;
    }
    const std::string &xattr = meta.user_meta("bosfs-xattr");
    size_t next = 0;
    size_t delim_pos = 0;
    size_t pos = locate_xattr(xattr, name, &next, &delim_pos);
    if (pos == std::string::npos) {
        return -ENOATTR;
    }
    if (next != 0 && xattr[next - 1] == ';') {
        --next;
    }
    std::string binary;
    if (delim_pos != std::string::npos) {
        binary = StringUtil::base64_decode(xattr.substr(delim_pos + 1, next - delim_pos - 1));
    }
    if (size != 0) {
        if (size < binary.length()) {
            return -ERANGE;
        }
        binary.copy(value, binary.length());
    }
    return binary.length();
}

END_FS_NAMESPACE
