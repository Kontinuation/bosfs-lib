/**
 * bosfs - A fuse-based file system implemented on Baidu Object Storage(BOS)
 *
 * Copyright (c) 2020 Baidu.com, Inc. All rights reserved.
 *
 * @file    sys_util.h
 * @author  pengbo09@baidu.com
 * @date    2020.9
 **/
#include "sys_util.h"
#include "bosfs_lib/bosfs_lib.h"

#include <stdlib.h>    /* for malloc/free */
#include <unistd.h>    /* for sysconf, geteuid... */
#include <errno.h>     /* for error number */
#include <libgen.h>    /* for dirname/basename */
#include <pwd.h>      /* for passwd */
#include <grp.h>      /* for group */
#include <dirent.h>
#include <fstream>
#include <sstream>
#include <pthread.h>

#include <sys/types.h>
#include <sys/stat.h>

BEGIN_FS_NAMESPACE

const char *        SysUtil::_s_default_mime_file = "/etc/mime.types";
SysUtil::mimes_t  SysUtil::_s_mime_types;
bool SysUtil::_s_is_mime_types_initialized = false;

std::string SysUtil::get_username(uid_t uid)
{
    std::string ret("");
    static size_t max_len = 0;
    if (0 == max_len) {
        long res = sysconf(_SC_GETPW_R_SIZE_MAX);
        if (res < 0) {
            BOSFS_WARN("%s", "Could not get max pw length!");
            max_len = 0;
            return ret;
        }
        max_len = res;
    }

    char *buf = NULL;
    if (NULL == (buf = (char *)malloc(sizeof(char) * max_len))) {
        BOSFS_ERR("%s", "Failed to allocate memory!");
        free(buf);
        return ret;
    }

    struct passwd pwinfo;
    struct passwd *ppwinfo = NULL;
    if (0 != getpwuid_r(uid, &pwinfo, buf, max_len, &ppwinfo)) {
        BOSFS_WARN("%s", "Could not get pw information!");
        free(buf);
        return ret;
    }

    if (NULL == ppwinfo) {
        free(buf);
        return ret;
    }
    ret = ppwinfo->pw_name;
    free(buf);
    return ret;
}

int SysUtil::is_uid_in_group(uid_t uid, gid_t gid)
{
    int result = 0;
    static size_t max_len = 0;
    if (0 == max_len) {
        long res = sysconf(_SC_GETGR_R_SIZE_MAX);
        if (res < 0) {
            BOSFS_ERR("%s", "Could not get max name length!");
            max_len = 0;
            return -ERANGE;
        }
        max_len = res;
    }

    char * buf = NULL;
    if (NULL ==(buf = (char *)malloc(sizeof(char) * max_len))) {
        BOSFS_ERR("%s", "Failed to allocate memory!");
        free(buf);
        return -ENOMEM;
    }

    struct group ginfo;
    struct group *pginfo = NULL;
    if (0 != (result = getgrgid_r(gid, &ginfo, buf, max_len, &pginfo))) {
        BOSFS_ERR("%s", "Could not get group information!");
        free(buf);
        return -result;
    }

    if (NULL == pginfo) {
        free(buf);
        return -EINVAL;
    }

    std::string username = get_username(uid);
    char ** ppgr;
    for (ppgr = pginfo->gr_mem; ppgr && *ppgr; ++ppgr) {
        if (username == *ppgr) {
            free(buf);
            return 1;
        }
    }
    free(buf);
    return 0;
}

std::string SysUtil::bosfs_basename(const char *path)
{
    if (!path || '\0' == path[0]) {
        return std::string("");
    }
    return bosfs_basename(std::string(path));
}

std::string SysUtil::bosfs_basename(std::string path)
{
    return std::string(basename((char *)path.c_str()));
}

int SysUtil::mkdirp(const std::string &path, mode_t mode)
{
    std::string base;
    std::string component;
    std::stringstream ss(path);
    while (getline(ss, component, '/')) {
        base += "/" + component;

        struct stat st;
        if (0 == stat(base.c_str(), &st)) {
            if (!S_ISDIR(st.st_mode)) {
                return EPERM;
            }
        } else {
            if (0 != mkdir(base.c_str(), mode)) {
                return errno;
            }
        }
    }
    return 0;
}

bool SysUtil::check_exist_dir_permission(const char *dirpath)
{
    if (!dirpath || '\0' == dirpath[0]) {
        return false;
    }

    struct stat st;
    if (0 != stat(dirpath, &st)) {
        if (ENOENT == errno) {
            return true;
        }
        if (EACCES == errno) {
            return false;
        }
        return false;
    }

    if (!S_ISDIR(st.st_mode)) {
        return false;
    }

    uid_t uid = geteuid();
    if (uid == st.st_uid) {
        if (S_IRWXU != (st.st_mode & S_IRWXU)) {
            return false;
        }
    } else {
        if (1 == is_uid_in_group(uid, st.st_gid)) {
            if (S_IRWXG != (st.st_mode & S_IRWXG)) {
                return false;
            }
        } else {
            if (S_IRWXO != (st.st_mode & S_IRWXO)) {
                return false;
            }
        }
    }
    return true;
}

bool SysUtil::delete_files_in_dir(const char *dir, bool is_remove_own)
{
    DIR * pdir = NULL;
    struct dirent * dent;

    if (NULL == (pdir = opendir(dir))) {
        BOSFS_ERR("Could not open dir(%s) - errno(%d)", dir, errno);
        return false;
    }

    for (dent = readdir(pdir); dent; dent = readdir(pdir)) {
        if (0 == strcmp(dent->d_name, "..") || 0 == strcmp(dent->d_name, ".")) {
            continue;
        }

        std::string fullpath = dir;
        fullpath += "/";
        fullpath += dent->d_name;
        struct stat st;
        if (0 != lstat(fullpath.c_str(), &st)) {
            BOSFS_ERR("Could not open get stat of file(%s) - errno(%d)", fullpath.c_str(), errno);
            closedir(pdir);
            return false;
        }
        if (S_ISDIR(st.st_mode)) {
            if (!delete_files_in_dir(fullpath.c_str(), true)) {
                BOSFS_ERR("Could not remove sub dir(%s) - errno(%d)", fullpath.c_str(), errno);
                closedir(pdir);
                return false;
            }
        } else {
            if (0 != unlink(fullpath.c_str())) {
                BOSFS_ERR("Could not remove files(%s) - errno(%d)", fullpath.c_str(), errno);
                closedir(pdir);
                return false;
            }
        }
    }
    closedir(pdir);

    if (is_remove_own && 0 != rmdir(dir)) {
        BOSFS_ERR("Could not remove dir(%s) - errno(%d)", dir, errno);
        return false;
    }
    return true;
}

int SysUtil::check_local_dir(const char *name, const std::string &localdir, std::string &errmsg) {
    struct stat st;
    if (stat(localdir.c_str(), &st) != 0) {
        return return_with_error_msg(
            errmsg, "unable to access local directory of %s:%s, error:%s", name, localdir.c_str(),
            strerror(errno));
    }
    if (!(S_ISDIR(st.st_mode))) {
        return return_with_error_msg(
            errmsg, "local directory of %s:%s is not a directory", name, localdir.c_str());
    }
    char buffer[10240];
    if (realpath(localdir.c_str(), buffer) == NULL) {
        return return_with_error_msg(
            errmsg, "unable get absolute path of local directory %s:%s, errno:%d", name, localdir.c_str(),
            errno);
    }
    return 0;
}

bool SysUtil::init_mimetype(const char *mime_file)
{
    if (_s_is_mime_types_initialized) {
        return true;
    }

    if (!mime_file) {
        mime_file = _s_default_mime_file;
    }
    std::string line;
    std::ifstream ifs(mime_file);
    if (ifs.good()) {
        while (std::getline(ifs, line)) {
            if (line.size() == 0 || line[0] == '#') {
                continue;
            }

            std::stringstream tmp(line);
            std::string mimetype;
            tmp >> mimetype;
            while (tmp) {
                std::string ext;
                tmp >> ext;
                if (ext.size() == 0) {
                    continue;
                }
                _s_mime_types[ext] = mimetype;
            }
        }
    }
    _s_is_mime_types_initialized = true;
    return true;
}

std::string SysUtil::get_mimetype(const std::string &path_name)
{
    std::string result("application/octet-stream");
    std::string::size_type last_pos = path_name.find_last_of('.');
    std::string::size_type first_pos = path_name.find_first_of('.');
    std::string prefix;
    std::string ext;
    std::string ext2;

    if (last_pos == std::string::npos) {
        return result;
    } else {
        ext = path_name.substr(last_pos + 1, std::string::npos);

        if (first_pos != std::string::npos && first_pos < last_pos) {
            prefix = path_name.substr(0, last_pos);
            std::string::size_type next_pos = prefix.find_last_of('.');
            if (next_pos != std::string::npos) {
                ext2 = prefix.substr(next_pos + 1, std::string::npos);
            }
        }
    }

    // check the first ext
    mimes_t::const_iterator iter = _s_mime_types.find(ext);
    if (iter != _s_mime_types.end()) {
        result = iter->second;
        return result;
    }

    // check the second ext
    if (first_pos == last_pos) {
        return result;
    }
    iter = _s_mime_types.find(ext2);
    if (iter != _s_mime_types.end()) {
        result = iter->second;
    }
    return result;
}

END_FS_NAMESPACE
