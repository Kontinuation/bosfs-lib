/**
 * bosfs - A fuse-based file system implemented on Baidu Object Storage(BOS)
 *
 * Copyright (c) 2020 Baidu.com, Inc. All rights reserved.
 *
 * @file    sys_util.h
 * @author  pengbo09@baidu.com
 * @date    2020.9
 **/
#ifndef BAIDU_BOS_BOSFS_SYS_UTIL_H
#define BAIDU_BOS_BOSFS_SYS_UTIL_H

#include <stdint.h>
#include <string.h>
#include <time.h>

#include <string>
#include <map>

#include "common.h"
#include "util.h"

BEGIN_FS_NAMESPACE

// Directory type
enum dir_type_t {
    DIR_UNKNOWN  = -1,
    DIR_NEW      = 0,
    DIR_OLD      = 1,
    DIR_FOLDER   = 2,
    DIR_NOOBJ    = 3
};

class SysUtil {
private:
    class CaseInsensitiveComp {
    public:
        bool operator()(const std::string &a, const std::string &b) const {
            return strcasecmp(a.c_str(), b.c_str()) < 0;
        }
    };

public:
    typedef std::map<std::string, std::string, CaseInsensitiveComp> mimes_t;

public:
    static inline bool is_replace_dir(int type) {
        return type == DIR_OLD || type == DIR_FOLDER || type == DIR_NOOBJ;
    }
    static inline bool is_empty_dir(int type) {
        return type == DIR_OLD || type == DIR_FOLDER;
    }
    static inline bool is_dir_path(const std::string &path) {
        return path[path.length() - 1] == '/';
    }

    static std::string get_username(uid_t uid);
    static int is_uid_in_group(uid_t uid, gid_t gid);
    static std::string bosfs_basename(const char *path);
    static std::string bosfs_basename(std::string path);

    static int mkdirp(const std::string &path, mode_t mode);
    static bool check_exist_dir_permission(const char *dirpath);
    static bool delete_files_in_dir(const char *dir, bool is_remove_own);
    static int check_local_dir(const char *name, const std::string &localdir, std::string &errmsg);

    static bool init_mimetype(const char *mime_file=NULL);
    static std::string get_mimetype(const std::string &path_name);

private:
    static const char * _s_default_mime_file;
    static mimes_t _s_mime_types;
    static bool _s_is_mime_types_initialized;
};

END_FS_NAMESPACE

#endif
