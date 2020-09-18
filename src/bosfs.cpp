/**
 * bosfs - A fuse-based file system implemented on Baidu Object Storage(BOS)
 *
 * Copyright (c) 2016 Baidu.com, Inc. All rights reserved.
 *
 * @file    bosfs.cpp
 * @author  Song Shuangyang(songshuangyang@baidu.com)
 * @date    2016.8
 **/

#include "bosfs.h"
#include "file_manager.h"
#include <string>
#include <limits.h>
#include <sys/xattr.h>

BEGIN_FS_NAMESPACE

// Common global variable definition
std::string g_program_name       = "bosfs";
std::string g_endpoint;
std::string g_bucket             = "";
std::string g_bucket_prefix      = "";
std::string g_ak                 = "";
std::string g_sk                 = "";
std::string g_sts_token;
std::string g_storage_class;
std::string g_credentials_path;

int64_t g_multipart_size = 10 * 1024 * 1024;
int g_multipart_parallel = 10;
int64_t g_multipart_threshold = 100 * 1024 * 1024;

// Variable for command arguments
time_t g_mount_time = 0;
uid_t g_mount_uid                = 0;
gid_t g_mount_gid                = 0;
mode_t g_mount_mode              = 0;
mode_t g_mount_umask             = 0022;
bool g_is_mount_umask            = false;
std::string g_mountpoint;
bool g_allow_other               = false;
uid_t g_bosfs_uid                = 0;
gid_t g_bosfs_gid                = 0;
mode_t g_bosfs_mask              = 0;
bool g_is_bosfs_uid              = false;
bool g_is_bosfs_gid              = false;
bool g_is_bosfs_umask            = false;
bool g_remove_cache              = false;
bool g_create_bucket             = false;
uint64_t g_bos_client_timeout    = 1200;

void *Bosfs::init(struct fuse_conn_info *conn) {
    BOSFS_INFO("fuse init");

#ifndef __APPLE__
    if (static_cast<unsigned int>(conn->capable) & FUSE_CAP_ATOMIC_O_TRUNC) {
        conn->want |= FUSE_CAP_ATOMIC_O_TRUNC;
    }
#endif
    return NULL;
}

void Bosfs::destroy(void *) {
    BOSFS_INFO("fuse destroy");
}

int Bosfs::access(const char *path, int mask) {

    std::string realpath = BosfsUtil::get_real_path(path);
    path = realpath.c_str();
    BOSFS_INFO("access[path=%s][mask=%s%s%s%s]", path,
            ((mask & R_OK) == R_OK) ? "R_OK" : "",
            ((mask & W_OK) == W_OK) ? "W_OK" : "",
            ((mask & X_OK) == X_OK) ? "X_OK" : "",
            (mask == F_OK) ? "F_OK" : "");
    int ret = BosfsUtil::check_object_access(path, mask, NULL);
    return ret;
}

int Bosfs::create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    BOSFS_INFO("create [path=%s][mode=%04o][flags=%d]", path, mode, fi->flags);
    std::string realpath = BosfsUtil::get_real_path(path);
    path = realpath.c_str();
    int ret = BOSFS_OK;
    struct fuse_context *pctx = fuse_get_context();
    if (NULL == pctx) {
        return -EIO;
    }
    ret = BosfsUtil::check_parent_object_access(path, X_OK | W_OK);
    if (ret != 0) {
        return ret;
    }

    ScopedPtr<ObjectMetaData> meta(new ObjectMetaData());
    BosfsUtil::create_meta(path, mode, pctx->uid, pctx->gid, meta.get());

    DataCacheEntity *ent = DataCache::instance()->open_cache(path, meta.get(), 0, -1, false, true);
    if (ent == NULL) {
        return -EIO;
    }
    ent->set_modified(true);
    FilePtr file(new File(path));
    file->meta().move_from(*meta);
    FileManager::instance().set(path, file);
    fi->fh = (int64_t) ent;
    return 0;
}

int Bosfs::open(const char *path, struct fuse_file_info *fi)
{
    BOSFS_INFO("open [path=%s][flags=%d]", path, fi->flags);
    std::string realpath = BosfsUtil::get_real_path(path);
    path = realpath.c_str();
    int ret = BosfsUtil::check_parent_object_access(path, X_OK);
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
    ret = BosfsUtil::check_object_access(path, access, NULL);
    if (0 != ret) {
        return ret;
    }
    struct stat st;
    ObjectMetaData meta;
    ret = BosfsUtil::get_object_attribute(path, &st, &meta);
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

    DataCacheEntity *ent = DataCache::instance()->open_cache(
            path, &meta, static_cast<ssize_t>(st.st_size), st.st_mtime, false, true);
    if (ent == NULL)  {
        FileManager::instance().del(path);
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

int Bosfs::read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    BOSFS_INFO("read [path=%s][size=%u][offset=%ld][fd=%lx]", path, size, offset, fi->fh);
    DataCacheEntity *ent = (DataCacheEntity *) fi->fh;
    size_t real_size = 0;
    if (!ent->get_size(real_size) || real_size <= 0) {
        BOSFS_DEBUG("%s", "file size is 0, break to read");
        return 0;
    }
    return ent->read(buf, offset, size, false);
}

int Bosfs::write(const char *path, const char *buf, size_t size,
        off_t offset, struct fuse_file_info *fi) {
    BOSFS_INFO("[path=%s][size=%u][offset=%ld][fd=%lx]", path, size, offset, fi->fh);
    DataCacheEntity *ent = (DataCacheEntity *) fi->fh;
    return ent->write(buf, offset, size);
}

int Bosfs::flush(const char *path, struct fuse_file_info *fi) {
    BOSFS_INFO("[path=%s][fh=%lx]", path, fi->fh);
    DataCacheEntity *ent = (DataCacheEntity *) fi->fh;
    ent->update_mtime();
    if (ent->flush(false) != 0) {
        return -EIO;
    }
    return 0;
}

int Bosfs::fsync(const char *path, int isdatasync, struct fuse_file_info *fi) {
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

int Bosfs::release(const char *path, struct fuse_file_info *fi) {
    BOSFS_INFO("fuse RELEASE: path:%s", path);

    DataCacheEntity *ent = (DataCacheEntity *) fi->fh;
    DataCache::instance()->close_cache(ent);
    return 0;
}

int Bosfs::statfs(const char *path, struct statvfs *stbuf) {
    (void) path;
    // 256TB
    stbuf->f_bsize   = 0x1000000;
    stbuf->f_blocks  = 0x1000000;
    stbuf->f_bfree   = 0x1000000;
    stbuf->f_bavail  = 0x1000000;
    stbuf->f_namemax = NAME_MAX;
    return 0;
}

int Bosfs::symlink(const char *target, const char *path) {
    BOSFS_INFO("symlink %s -> %s", path, target);
    std::string realpath = BosfsUtil::get_real_path(path);
    path = realpath.c_str();
    std::string real_targetpath = BosfsUtil::get_real_path(target);
    target = real_targetpath.c_str();

    struct fuse_context *pctx = fuse_get_context();
    if (pctx == NULL) {
        return -EIO;
    }
    int ret = 0;
    if (0 != (ret = BosfsUtil::check_parent_object_access(path, W_OK | X_OK))) {
        return ret;
    }
    ret = BosfsUtil::check_object_access(path, F_OK, NULL);
    if (ret != -ENOENT) {
        if (ret == 0) {
            return -EEXIST;
        }
        return ret;
    }
    mode_t mode = S_IFLNK | S_IRWXU | S_IRWXG | S_IRWXO;
    ret = BosfsUtil::create_object(path, mode, pctx->uid, pctx->gid, target);
    if (ret != 0) {
        return -EIO;
    }
    FileManager::instance().del(path);
    return 0;
}

int Bosfs::link(const char *from, const char *to) {//not implemented
    (void) from;
    (void) to;
    return -EPERM;
}

int Bosfs::unlink(const char *path) {
    // search permission of path components and write permission of parent directory
    std::string realpath = BosfsUtil::get_real_path(path);
    path = realpath.c_str();
    int ret = BosfsUtil::check_parent_object_access(path, W_OK | X_OK);
    if (ret != 0) {
        return ret;
    }
    ret = BosfsUtil::delete_object(path + 1);
    FileManager::instance().del(path);
    if (ret != 0) {
        return ret;
    }
    DataCache::instance()->delete_cache_file(path);
    return 0;
}

int Bosfs::readlink(const char *path, char *buf, size_t size) {
    if (!buf || 0 >= size) {
        return 0;
    }

    DataCacheEntity *ent = NULL;
    if (NULL == (ent = BosfsUtil::get_local_entity(path))) {
        BOSFS_ERR("could not get entity(file = %s)", path);
        return -EIO;
    }

    size_t read_size = 0;
    ent->get_size(read_size);

    if (size <= read_size) {
        read_size = size - 1;
    }

    int ret = ent->read(buf, 0, read_size);
    DataCache::instance()->close_cache(ent);
    if (ret < 0) {
        BOSFS_ERR("could not read file(file=%s, errno=%d)", path, ret);
        return ret;
    }
    buf[ret] = '\0';
    return 0;
}

int Bosfs::mknod(const char *path, mode_t mode, dev_t rdev) {
    BOSFS_INFO("mknod [path=%s][mode=%04o][dev=%ju]", path, mode, rdev);
    std::string realpath = BosfsUtil::get_real_path(path);
    path = realpath.c_str();
    struct fuse_context *pctx = NULL;
    if (NULL == (pctx = fuse_get_context())) {
        return -EIO;
    }
    int ret = BosfsUtil::check_parent_object_access(path, W_OK | X_OK);
    if (ret != 0) {
        return ret;
    }
    // check existence
    struct stat st;
    ret = BosfsUtil::get_object_attribute(path, &st);
    if (ret != -ENOENT) {
        if (ret == 0) {
            return -EEXIST;
        }
        return ret;
    }

    ret = BosfsUtil::create_object(path, mode, pctx->uid, pctx->gid);
    if (ret != 0) {
        BOSFS_ERR("could not create object for special file, result = %d", ret);
        return -EIO;
    }
    FileManager::instance().del(path);
    return ret;
}

int Bosfs::mkdir(const char *path, mode_t mode) {
    BOSFS_INFO("mkdir [path=%s][mode=%04o]", path, mode);
    std::string realpath = BosfsUtil::get_real_path(path);
    path = realpath.c_str();
    struct fuse_context *pctx = NULL;
    if (NULL == (pctx = fuse_get_context())) {
        return -EIO;
    }

    int ret = BosfsUtil::check_parent_object_access(path, W_OK | X_OK);
    if (ret != 0) {
        return ret;
    }
    // check existence
    struct stat st;
    ret = BosfsUtil::get_object_attribute(path, &st);
    if (ret != -ENOENT) {
        if (ret == 0) {
            return -EEXIST;
        }
        return ret;
    }

    //ret = BosfsUtil::create_directory_object(path, mode, pctx->uid, pctx->gid);
    ret = BosfsUtil::create_object(path, mode | S_IFDIR, pctx->uid, pctx->gid);
    if (ret != 0) {
        return -EIO;
    }
    FileManager::instance().del(path);
    return 0;
}

int Bosfs::rmdir(const char *path) {
    std::string realpath = BosfsUtil::get_real_path(path);
    path = realpath.c_str();
    std::string object_name(path + 1);
    // forbid delete mountpoint
    if (object_name.empty()) {
        return -EPERM;
    }

    int ret = BosfsUtil::check_parent_object_access(path, W_OK | X_OK);
    if (ret != 0) {
        return ret;
    }

    // Directory must be empty
    std::vector<std::string> subitems;
    if (BosfsUtil::list_subitems(object_name + "/", 2, &subitems) != 0) {
        return -EIO;
    }
    if (!subitems.empty()) {
        return -ENOTEMPTY;
    }
    ret = BosfsUtil::delete_object(object_name + "/");
    if (ret != 0) {
        if (ret == BOSFS_OBJECT_NOT_EXIST) {
            ret = BosfsUtil::delete_object(object_name);
        }
    }
    // force refresh meta info, may be delete is success but response is timeout
    FileManager::instance().del(path);
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
int Bosfs::rename(const char *from, const char *to) {
    BOSFS_INFO("rename [from=%s][to=%s]", from, to);
    std::string realpath_from = BosfsUtil::get_real_path(from);
    from = realpath_from.c_str();
    std::string realpath_to = BosfsUtil::get_real_path(to);
    to = realpath_to.c_str();
    int ret = 0;
    // unlink file in src
    ret = BosfsUtil::check_parent_object_access(to, W_OK | X_OK);
    if (ret != 0) {
        return ret;
    }
    // unlink(when need overwrite) and create file in dst
    ret = BosfsUtil::check_parent_object_access(from, W_OK | X_OK);
    if (ret != 0) {
        return ret;
    }

    // existence of src
    struct stat st;
    ret = BosfsUtil::get_object_attribute(from, &st);
    if (ret != 0) {
        return ret;
    }

    if (S_ISDIR(st.st_mode)) { // rename directory
        ret = BosfsUtil::rename_directory(from + 1, to + 1);
    } else {
        ret = BosfsUtil::rename_file(from + 1, to + 1, st.st_size);
    }
    if (0 != ret) {
        BOSFS_ERR("rename failed, from: %s, to: %s", from, to);
        return -EIO;
    }
    return 0;
}


int Bosfs::opendir(const char *path, struct fuse_file_info *fi) {
    BOSFS_INFO("opendir [path=%s][flags=%d]", path, fi->flags);
    // do nothing
    std::string realpath = BosfsUtil::get_real_path(path);
    path = realpath.c_str();
    int ret = BosfsUtil::check_path_accessible(path);
    if (ret != 0) {
        return ret;
    }
    return BosfsUtil::check_object_access(path, R_OK, NULL);
}

int Bosfs::readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
        struct fuse_file_info *fi) {
    (void) fi;
    LOG(INFO) << "readdir, path:" << path << " offset:" << offset;
    std::string path_(path+1);
    std::string prefix = g_bucket_prefix + path_;
    if (!path_.empty()) {
        prefix = prefix + "/";
    }
    std::string marker;
    do {
        std::vector<std::string> items;
        std::vector<std::string> prefixes;
        int ret = BosfsUtil::list_objects(prefix, 1000, marker, "/", &items, &prefixes);
        if (ret != 0) {
            return -EIO;
        }
        struct stat default_st;
        BosfsUtil::init_default_stat(&default_st);
        for (size_t i = 0; i < prefixes.size(); ++i) {
            std::string dir_path = BosfsUtil::object_to_path(prefixes[i]);
            FilePtr file(new File(dir_path));
            file->set_is_prefix(true);
            FileManager::instance().set(dir_path, file);
            std::string basename = BosfsUtil::object_to_basename(prefixes[i], prefix);
            if (filler(buf, basename.c_str(), &default_st, 0)) {
                break;
            }
        }
        std::vector<struct stat> stats(items.size());
        std::vector<std::string> no_cache_items;
        std::vector<struct stat *> no_cache_stats;
        for (size_t i = 0; i < items.size(); ++i) {
            FilePtr file;
            if (FileManager::instance().try_get(BosfsUtil::object_to_path(items[i]), &file)) {
                file->stat(&stats[i]);
            } else {
                no_cache_items.push_back(items[i]);
                no_cache_stats.push_back(&stats[i]);
            }
        }
        BosfsUtil::multiple_head_object(no_cache_items, no_cache_stats);
        for (size_t i = 0; i < items.size(); ++i) {
            std::string basename = BosfsUtil::object_to_basename(items[i], prefix);
            if (filler(buf, basename.c_str(), &stats[i], 0)) {
                break;
            }
        }
    } while (!marker.empty());
    return 0;
}

int Bosfs::releasedir(const char* path, struct fuse_file_info *fi) {
    (void) path;
    (void) fi;
    return 0;
}

int Bosfs::chmod(const char *path, mode_t mode)
{
    BOSFS_INFO("chmod [path=%s][mode=%04o]", path, mode);
    std::string realpath = BosfsUtil::get_real_path(path);
    path = realpath.c_str();
    if (0 == strcmp(path, "/")) {
        LOG(ERROR) << "ignored chmod for mountpoint, path:" << path << " mode:" << mode;
        return 0;
    }
    int ret = BosfsUtil::check_path_accessible(path);
    if (ret != 0) {
        return ret;
    }
    struct stat st;
    ret = BosfsUtil::check_object_owner(path, &st);
    if (ret != 0) {
        return ret;
    }
    ObjectMetaData meta;
    ret = BosfsUtil::get_object_attribute(path, &st, &meta);
    if (ret != 0) {
        return ret;
    }
    std::string object_name(path + 1);
    if (S_ISDIR(st.st_mode)) {
        object_name += '/';
    }
    meta.set_user_meta("bosfs-mode", mode);
    ret = BosfsUtil::change_object_meta(object_name, meta);
    if (ret != 0) {
        if (ret == -ENOENT) {
            DataCacheEntity *ent = DataCache::instance()->exist_open(path);
            if (ent != NULL) {
                ent->set_mode(mode);
                DataCache::instance()->close_cache(ent);
                return 0;
            }
        }
        return ret;
    }
    FileManager::instance().del(path);
    return 0;
}

int Bosfs::chown(const char *path, uid_t uid, gid_t gid) {
    std::string realpath = BosfsUtil::get_real_path(path);
    path = realpath.c_str();
    if (0 == strcmp(path, "/")) {
        LOG(ERROR) << "ignored chown for mountpoint, path:" << path << " uid:" << uid << " gid:"
            << gid;
        return 0;
    }
    int ret = BosfsUtil::check_path_accessible(path);
    if (ret != 0) {
        return ret;
    }
    
    struct fuse_context *pctx = fuse_get_context();
    if (pctx == NULL) {
        return -EIO;
    }
    // only root can chown
    if (pctx->uid != 0) {
        return -EPERM;
    }
    struct stat st;
    ObjectMetaData meta;
    ret = BosfsUtil::get_object_attribute(path, &st, &meta);
    if (ret != 0) {
        return ret;
    }
    DataCacheEntity *ent = DataCache::instance()->exist_open(path);
    if (ent != NULL) {
        ent->set_uid(uid);
        ent->set_gid(gid);
        DataCache::instance()->close_cache(ent);
        return 0;
    }
    std::string object_name(path + 1);
    if (S_ISDIR(st.st_mode)) {
        object_name += '/';
    }
    meta.set_user_meta("bosfs-uid", uid);
    meta.set_user_meta("bosfs-gid", gid);
    ret = BosfsUtil::change_object_meta(object_name, meta);
    if (ret != 0) {
        return ret;
    }
    FileManager::instance().del(path);
    return 0;
}

int Bosfs::utimens(const char *path, const struct timespec ts[2]) {
    std::string realpath = BosfsUtil::get_real_path(path);
    path = realpath.c_str();
    if (strcmp(path, "/") == 0) {
        g_mount_time = ts[1].tv_sec;
        return 0;
    }
    int ret = BosfsUtil::check_path_accessible(path);
    if (ret != 0) {
        return ret;
    }
    struct stat st;
    ret = BosfsUtil::check_object_access(path, W_OK, &st);
    if (ret != 0) {
        return ret;
    }
    ret = BosfsUtil::check_object_owner(path, &st);
    if (ret != 0) {
        return ret;
    }
    ObjectMetaData meta;
    ret = BosfsUtil::get_object_attribute(path, &st, &meta);
    if (ret != 0) {
        return ret;
    }
    std::string object_name(path + 1);
    if (S_ISDIR(st.st_mode)) {
        object_name += '/';
    }
    DataCacheEntity *ent = DataCache::instance()->exist_open(path);
    if (ent != NULL) {
	ent->set_mode(strtol(meta.user_meta("bosfs-mode").c_str(), NULL, 0));
        ret = ent->set_mtime(ts[1].tv_sec);
        DataCache::instance()->close_cache(ent);
        return ret;
    }
    meta.set_user_meta("bosfs-mtime", ts[1].tv_sec);
    ret = BosfsUtil::change_object_meta(object_name, meta);
    if (ret != 0) {
        return ret;
    }
    FileManager::instance().del(path);
    return 0;
}

int Bosfs::getattr(const char *path, struct stat *stbuf) {
    // st t() requires path executable
    std::string realpath = BosfsUtil::get_real_path(path);
    path = realpath.c_str();
    int ret = BosfsUtil::check_path_accessible(path);
    if (ret != 0) {
        return ret;
    }
    // check existence
    ret = BosfsUtil::get_object_attribute(path, stbuf);
    if (ret != 0) {
        return ret;
    }
    if (stbuf == NULL) {
        return 0;
    }
    if (!S_ISREG(stbuf->st_mode)) {
        return 0;
    }
    DataCacheEntity *ent = DataCache::instance()->exist_open(path);
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
        DataCache::instance()->close_cache(ent);
    }
    return 0;
}

int Bosfs::truncate(const char* path, off_t size) {
    std::string realpath = BosfsUtil::get_real_path(path);
    path = realpath.c_str();
    BOSFS_INFO("truncate file %s to %ld", path, size);
    int ret = BosfsUtil::check_path_accessible(path);
    if (ret != 0) {
        return ret;
    }
    ret = BosfsUtil::check_object_access(path, W_OK, NULL);
    if (0 != ret) {
        return ret;
    }
    struct stat st;
    ObjectMetaData meta;
    ret = BosfsUtil::get_object_attribute(path, &st, &meta);
    if (ret != 0) {
        return ret;
    }
    DataCacheEntity *ent = DataCache::instance()->open_cache(path, &meta, st.st_size, st.st_mtime);
    if (ent == NULL) {
        return -EIO;
    }
    ret = ent->truncate(size);
    if (ret != 0) {
        DataCache::instance()->close_cache(ent);
        return ret;
    }
    ret = ent->load(0, size);
    if (ret != 0) {
        DataCache::instance()->close_cache(ent);
        return ret;
    }
    ret = ent->flush(true);
    if (ret != 0) {
        DataCache::instance()->close_cache(ent);
        return ret;
    }
    DataCache::instance()->close_cache(ent);
    FileManager::instance().del(path);
    return 0;
}

/*
 * use to extract a key-value string in a xattr string;
 * if name was empty then match every key, or else exactly match the name.
 * @param next        will be set as postition of the name should be insert into
 * @param delim_pos   if matched then will be set as the position of ':' or npos
 * @return npos on not matched, and start pos of matched key-value string
 **/
size_t locate_xattr(const std::string &xattr, const std::string &name, size_t *next,
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

int Bosfs::listxattr(const char *path, char *buffer, size_t size) {
    std::string realpath = BosfsUtil::get_real_path(path);
    path = realpath.c_str();
    int ret = BosfsUtil::check_path_accessible(path);
    if (ret != 0) {
        return ret;
    }
    struct stat st;
    ObjectMetaData meta;
    // check existence
    ret = BosfsUtil::get_object_attribute(path, &st, &meta);
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

int Bosfs::removexattr(const char *path, const char *name)
{
    if (0 == strcmp(path, "/")) {
        LOG(ERROR) << "ignored removexattr for mountpoint, path:" << path << " name:" << name;
        return 0;
    }
    int ret = BosfsUtil::check_path_accessible(path);
    if (ret != 0) {
        return ret;
    }
    struct stat st;
    ret = BosfsUtil::check_object_owner(path, &st);
    if (ret != 0) {
        return ret;
    }
    ObjectMetaData meta;
    ret = BosfsUtil::get_object_attribute(path, &st, &meta);
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
    DataCacheEntity *ent = DataCache::instance()->exist_open(path);
    if (ent != NULL) {
        ent->set_xattr(xattr);
        DataCache::instance()->close_cache(ent);
        return ret;
    }
    std::string object_name(path + 1);
    if (S_ISDIR(st.st_mode)) {
        object_name += '/';
    }
    ret = BosfsUtil::change_object_meta(object_name, meta);
    if (ret != 0) {
        return ret;
    }
    FileManager::instance().del(path);
    return 0;
}

#if defined(__APPLE__)
int Bosfs::setxattr(const char *path, const char *name, const char *value, size_t size, int flag,
        uint32_t)
#else
int Bosfs::setxattr(const char *path, const char *name, const char *value, size_t size, int flag)
#endif
{
    std::string realpath = BosfsUtil::get_real_path(path);
    path = realpath.c_str();
    if (0 == strcmp(path, "/")) {
        LOG(ERROR) << "ignored setxattr for mountpoint, path:" << path << " name:" << name
            << " value:" << std::string(value, size);
        return 0;
    }
    int ret = BosfsUtil::check_path_accessible(path);
    if (ret != 0) {
        return ret;
    }
    struct stat st;
    ret = BosfsUtil::check_object_owner(path, &st);
    if (ret != 0) {
        return ret;
    }
    ObjectMetaData meta;
    ret = BosfsUtil::get_object_attribute(path, &st, &meta);
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
    DataCacheEntity *ent = DataCache::instance()->exist_open(path);
    if (ent != NULL) {
        ent->set_xattr(xattr);
        DataCache::instance()->close_cache(ent);
        return ret;
    }
    std::string object_name(path + 1);
    if (S_ISDIR(st.st_mode)) {
        object_name += '/';
    }
    ret = BosfsUtil::change_object_meta(object_name, meta);
    if (ret != 0) {
        return ret;
    }
    FileManager::instance().del(path);
    return 0;
}

#if defined(__APPLE__)
int Bosfs::getxattr(const char *path, const char *name, char *value, size_t size, uint32_t)
#else
int Bosfs::getxattr(const char *path, const char *name, char *value, size_t size)
#endif
{
    std::string realpath = BosfsUtil::get_real_path(path);
    path = realpath.c_str();
    int ret = BosfsUtil::check_path_accessible(path);
    if (ret != 0) {
        return ret;
    }
    struct stat st;
    ObjectMetaData meta;
    // check existence
    ret = BosfsUtil::get_object_attribute(path, &st, &meta);
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
