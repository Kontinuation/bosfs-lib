/***************************************************************************
 * 
 * Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
/**
 * @file src/file_manager.cpp
 * @author lili19(com@baidu.com)
 * @date 2017/09/12 09:35:27
 * @brief 
 *  
 **/
#include "bosfs_lib/bosfs_lib.h"
#include "file_manager.h"
#include "bosfs_util.h"
#include <algorithm>

BEGIN_FS_NAMESPACE

int File::load_meta_from_bos() {
    MutexGuard lock(&_mutex);
    int ret = _bosfs_util->head_object(_name.substr(1), &_meta, &_is_dir_obj, &_is_prefix);
    if (ret == 0) {
        _load_time_s = get_system_time_s();
    }
    return ret;
}

// compatible with old user meta which not has "bosfs-" prefix
int File::stat(struct stat *st) {
    MutexGuard lock(&_mutex);
    _bosfs_util->init_default_stat(st);
    if (_name == "/" || _is_prefix) {
        st->st_size = ST_BLKSIZE;
        st->st_blocks = ST_MINBLOCKS;
        return 0;
    }
    st->st_size = _meta.content_length();
    st->st_blocks = (st->st_size + st->st_blksize - 1) / st->st_blksize * ST_MINBLOCKS;
    const std::string *meta_mtime = &_meta.user_meta("bosfs-mtime");
    if (meta_mtime->empty()) {
        meta_mtime = &_meta.user_meta("mtime");
    }
    if (meta_mtime->empty()) {
        st->st_mtime = _meta.last_modified();
    } else {
        st->st_mtime = strtol(meta_mtime->c_str(), NULL, 0);
    }
    st->st_ctime = st->st_mtime;
    st->st_atime = st->st_mtime;
    const std::string *meta_uid = &_meta.user_meta("bosfs-uid");
    if (meta_uid->empty()) {
        meta_uid = &_meta.user_meta("uid");
    }
    if (!meta_uid->empty()) {
        st->st_uid = strtol(meta_uid->c_str(), NULL, 0);
    }
    const std::string *meta_gid = &_meta.user_meta("bosfs-gid");
    if (meta_gid->empty()) {
        meta_gid = &_meta.user_meta("gid");
    }
    if (!meta_gid->empty()) {
        st->st_gid = strtol(meta_gid->c_str(), NULL, 0);
    }
    bool is_dir = false;
    std::string content_type = _meta.content_type();
    if (_is_dir_obj && _meta.content_length() == 0) {
        is_dir = true;
    } else if (!content_type.empty()) {
        content_type = content_type.substr(0, content_type.find(';'));
        is_dir = (content_type == "application/x-directory");
    }
    st->st_mode = _bosfs_util->options().mount_mode;
    const std::string *meta_mode = &_meta.user_meta("bosfs-mode");
    if (meta_mode->empty()) {
        meta_mode = &_meta.user_meta("mode");
    }
    if (!meta_mode->empty()) {
        st->st_mode = strtol(meta_mode->c_str(), NULL, 0);
        if (!(st->st_mode & S_IFMT)) {//前四位表示文件类型
            st->st_mode |= is_dir ? S_IFDIR : S_IFREG;
        }
    } else {
        if (!is_dir) {
            st->st_mode &= ~(S_IFMT | 0111);//低三位
            st->st_mode |= S_IFREG;
        }
    }
    return 0;
}

int FileManager::get(const std::string &name, FilePtr *file) {
    if (try_get(name, file)) {
        return 0;
    }
    file->reset(new File(_bosfs_util, name));
    int ret = (*file)->load_meta_from_bos();
    if (ret != 0) {
        if (ret == BOSFS_OBJECT_NOT_EXIST) {
            return -ENOENT;
        }
        return -EIO;
    }
    size_t size = 0;
    {
        bcesdk_ns::TLSLockWriteGuard lock(_lock);
        FileTable::iterator it = _table.find(name);
        if (it != _table.end()) {
            *file = it->second;
        } else {
            _table[name] = *file;
        }
        size = _table.size();
    }
    if (_cache_capacity > 0 && size > (size_t) _cache_capacity) {
        gc();
    }
    return 0;
}

bool FileManager::try_get(const std::string &name, FilePtr *file) {
    int64_t now = get_system_time_s();
    bcesdk_ns::TLSLockReadGuard lock(_lock);
    FileTable::iterator it = _table.find(name);
    if (it == _table.end()) {
        return false;
    }
    if (_expire_s >= 0 && (it->second->load_time_s() + _expire_s) < now) {
        if (it->second.refcount() <= 1) {
            _table.erase(it);
            return false;
        }
    }
    it->second->hit(now);
    *file = it->second;
    return true;
}

void FileManager::set(const std::string &name, FilePtr &file) {
    size_t size = 0;
    {
        bcesdk_ns::TLSLockWriteGuard lock(_lock);
        _table[name] = file;
        size = _table.size();
    }
    if (_cache_capacity > 0 && size > (size_t) _cache_capacity) {
        gc();
    }
}

void FileManager::del(const std::string &name) {
    bcesdk_ns::TLSLockWriteGuard lock(_lock);
    _table.erase(name);
}

void FileManager::gc() {
    int64_t now = get_system_time_s();
    std::vector<FilePtr> candis;
    std::vector<FilePtr> removes;
    {
        bcesdk_ns::TLSLockReadGuard lock(_lock);
        for (FileTable::iterator it = _table.begin(); it != _table.end(); ++it) {
            if (_expire_s >= 0 && (it->second->load_time_s() + _expire_s) < now) {
                removes.push_back(it->second);
            } else {
                candis.push_back(it->second);
            }
        }
    }
    if (_cache_capacity >= 0 && candis.size() > (size_t) _cache_capacity) {
        std::sort(candis.begin(), candis.end(), compare_cache_priority);
        for (size_t i = _cache_capacity; i < candis.size(); ++i) {
            removes.push_back(candis[i]);
        }
    }
    candis.clear();

    for (size_t i = 0; i < removes.size(); ++i) {
        bcesdk_ns::TLSLockWriteGuard lock(_lock);
        FileTable::iterator it = _table.find(removes[i]->name());
        if (it == _table.end()) {
            continue;
        }
        if (it->second.refcount() > 2) {
            continue;
        }
        _table.erase(it);
    }
}

END_FS_NAMESPACE

