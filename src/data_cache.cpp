/**
 * bosfs - A fuse-based file system implemented on Baidu Object Storage(BOS)
 *
 * Copyright (c) 2016 Baidu.com, Inc. All rights reserved.
 *
 * @file    data_cache.cpp
 * @author  Song Shuangyang(songshuangyang@baidu.com)
 * @date    2016.9
 **/
#include <stdlib.h>
#include <sys/file.h>
#include <sys/time.h>
#include <utime.h>
#include <dirent.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <limits.h>

#include <string>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <exception>
#include <uuid/uuid.h>

#include "bosfs_lib/bosfs_lib.h"
#include "data_cache.h"
#include "util.h"
#include "bosfs_util.h"
#include "sys_util.h"
#include "file_manager.h"
#include "bcesdk/bos/client.h"

BEGIN_FS_NAMESPACE

// Definition of class ObjectPageList
void ObjectPageList::free_list(self_type &lst)
{
    for (self_type::iterator iter = lst.begin(); iter != lst.end(); iter = lst.erase(iter)) {
        delete *iter;
    }
    lst.clear();
}

ObjectPageList::ObjectPageList(size_t size, bool loaded)
{
    init(size, loaded);
}

ObjectPageList::~ObjectPageList()
{
    clear();
}

bool ObjectPageList::init(size_t size, bool loaded)
{
    clear();
    ObjectPage *page = new ObjectPage(0, size, loaded);
    _pages.push_back(page);
    return true;
}

size_t ObjectPageList::get_size() const
{
    if (_pages.empty()) {
        return 0;
    }
    self_type::const_reverse_iterator rite = _pages.rbegin();
    return static_cast<size_t>((*rite)->next());
}

bool ObjectPageList::resize(size_t size, bool loaded)
{
    size_t total = get_size();

    if (0 == total) {
        init(size, loaded);
    } else if (total < size) {
        ObjectPage *page = new ObjectPage(static_cast<off_t>(total), (size - total), loaded);
        _pages.push_back(page);
    } else if (size < total) {
        for (self_type::iterator iter = _pages.begin(); iter != _pages.end();) {
            if (static_cast<size_t>((*iter)->next()) <= size) {
                ++iter;
            } else {
                if (size <= static_cast<size_t>((*iter)->get_offset())) {
                    delete *iter;
                    iter = _pages.erase(iter);
                } else {
                    (*iter)->set_bytes(size - static_cast<size_t>((*iter)->get_offset()));
                }
            }
        }
    }
    return compress();
}

bool ObjectPageList::is_page_loaded(off_t start, size_t size) const
{
    for (self_type::const_iterator ite = _pages.begin(); ite != _pages.end(); ++ite) {
        if ((*ite)->end() < start) {
            continue;
        }
        if (!(*ite)->get_loaded()) {
            return false;
        }
        if (0 != size && static_cast<size_t>(start + size) <=
                static_cast<size_t>((*ite)->next())) {
            break;
        }
    }
    return true;
}

bool ObjectPageList::set_page_loaded_status(off_t start, size_t size, bool loaded, bool need_cmp)
{
    size_t now_size = get_size();

    if (now_size <= static_cast<size_t>(start)) {
        if (now_size < static_cast<size_t>(start)) {
            resize(static_cast<size_t>(start), false);
        }
        resize(static_cast<size_t>(start + size), loaded);
    } else if (now_size <= static_cast<size_t>(start + size)) {
        resize(static_cast<size_t>(start), false);
        resize(static_cast<size_t>(start + size), loaded);
    } else {
        parse(start);
        parse(start + size);

        for (self_type::iterator iter = _pages.begin(); iter != _pages.end(); ++iter) {
            if ((*iter)->end() < start) {
                continue;
            } else if (static_cast<off_t>(start + size) <= (*iter)->get_offset()) {
                break;
            } else {
                (*iter)->set_loaded(loaded);
            }
        }
    }
    return need_cmp ? compress() : true;
}

bool ObjectPageList::find_unloaded_pate(off_t start, off_t &ret_start, size_t &ret_size) const
{
    for (self_type::const_iterator iter = _pages.begin(); iter != _pages.end(); ++iter) {
        if (start < (*iter)->end()) {
            if (!(*iter)->get_loaded()) {
                ret_start = (*iter)->get_offset();
                ret_size  = (*iter)->get_bytes();
                return true;
            }
        }
    }
    return false;
}

size_t ObjectPageList::get_total_unloaded_page_size(off_t start, size_t size) const
{
    size_t ret_size = 0;
    off_t next = static_cast<off_t>(start + size);
    for (self_type::const_iterator iter = _pages.begin(); iter != _pages.end(); ++iter) {
        if ((*iter)->next() <= start) {
            continue;
        }
        if (next <= (*iter)->get_offset()) {
            break;
        }
        if ((*iter)->get_loaded()) {
            continue;
        }
        size_t tmp_size = 0;
        if ((*iter)->get_offset() <= start) {
            if ((*iter)->next() <= next) {
                tmp_size = static_cast<size_t>((*iter)->next() - start);
            } else {
                tmp_size = static_cast<size_t>(next - start);
            }
        } else {
            if ((*iter)->next() <= next) {
                tmp_size = static_cast<size_t>((*iter)->next() - (*iter)->get_offset());
            } else {
                tmp_size = static_cast<size_t>(next - (*iter)->get_offset());
            }
        }
        ret_size += tmp_size;
    }
    return ret_size;
}

int ObjectPageList::get_unloaded_pages(self_type &unloaded_list, off_t start, size_t size) const
{
    if (0 == size) {
        size_t real_size = get_size();
        if (static_cast<size_t>(start) < real_size) {
            size = real_size - start;
        }
    }
    off_t next = static_cast<off_t>(start + size);
    for (self_type::const_iterator iter = _pages.begin(); iter != _pages.end(); ++iter) {
        if ((*iter)->next() <= start) {
            continue;
        }
        if (next <= (*iter)->get_offset()) {
            break;
        }
        if ((*iter)->get_loaded()) {
            continue;
        }

        off_t page_start = std::max((*iter)->get_offset(), start);
        off_t page_next = std::min((*iter)->next(), next);
        size_t page_size = static_cast<size_t>(page_next - page_start);

        self_type::reverse_iterator rite = unloaded_list.rbegin();
        if (rite != unloaded_list.rend() && (*rite)->next() == page_start) {
            (*rite)->set_bytes((*rite)->get_bytes() + page_size);
        } else {
            ObjectPage *page = new ObjectPage(page_start, page_size, false);
            unloaded_list.push_back(page);
        }
    }

    return unloaded_list.size();
}

bool ObjectPageList::serialize(StatCacheFile &file, bool is_output) {
    if (!file.open_file()) {
        return false;
    }
    if (is_output) {
        std::stringstream ssall;
        ssall << get_size();

        for (self_type::iterator iter = _pages.begin(); iter != _pages.end(); ++iter) {
            ssall << "\n" << (*iter)->get_offset() << ":" << (*iter)->get_bytes() << ":"
                << ((*iter)->get_loaded() ? "1" : "0");
        }

        std::string strall = ssall.str();
        if (0 >= pwrite(file.get_fd(), strall.c_str(), strall.length(), 0)) {
            BOSFS_ERR("failed to write stats(%d)", errno);
            return false;
        }
    } else {
        struct stat st;
        memset(&st, 0, sizeof(struct stat));
        if (-1 == fstat(file.get_fd(), &st)) {
            BOSFS_ERR("fstat is failed. errno(%d)", errno);
            return false;
        }
        if (0 >= st.st_size) {
            init(0, false);
            return true;
        }
        SmartArray<char> tmp(st.st_size + 1);
        char *ptmp = tmp.get();

        if (0 >= pread(file.get_fd(), ptmp, st.st_size, 0)) {
            BOSFS_ERR("failed to read stats(%d)", errno);
            return false;
        }
        std::string       oneline;
        std::stringstream ssall(ptmp);

        clear();

        if (!std::getline(ssall, oneline, '\n')) {
            BOSFS_ERR("failed to parse stats.");
            return false;
        }
        size_t total = strtoll(oneline.c_str(), NULL, 10);

        bool is_err = false;
        while (std::getline(ssall, oneline, '\n')) {
            std::string       part;
            std::stringstream ssparts(oneline);

            if (!std::getline(ssparts, part, ':')) {
                is_err = true;
                break;
            }
            off_t offset = strtoll(part.c_str(), NULL, 10);

            if (!std::getline(ssparts, part, ':')) {
                is_err = true;
                break;
            }
            off_t size = strtoll(part.c_str(), NULL, 10);
            if (!std::getline(ssparts, part, ':')) {
                is_err = true;
                break;
            }
            bool is_loaded = strtoll(part.c_str(), NULL, 10) != 0;
            set_page_loaded_status(offset, size, is_loaded);
        }
        if (is_err) {
            BOSFS_ERR("failed to parse stats.");
            clear();
            return false;
        }

        if (total != get_size()) {
            BOSFS_ERR("different size(%jd - %jd).", (intmax_t) total, (intmax_t) get_size());
            clear();
            return false;
        }
    }
    return true;
}

void ObjectPageList::dump()
{
    int cnt = 0;
    std::ostringstream oss;
    oss << "pages = [";
    for (self_type::iterator it = _pages.begin(); it != _pages.end(); ++it) {
        ObjectPage *page = *it;
        if (cnt > 0) {
            oss << "->";
        }
        oss << "(off=" << page->get_offset() << ",size=" << page->get_bytes() <<
            ",load=" << page->get_loaded() << ")";
        ++cnt;
    }
    oss << "]";
    BOSFS_DEBUG("%s", oss.str().c_str());
}

void ObjectPageList::clear()
{
    ObjectPageList::free_list(_pages);
}
//合并page
bool ObjectPageList::compress()
{
    bool is_first = true;
    bool is_last_loaded = false;
    for (self_type::iterator iter = _pages.begin(); iter != _pages.end();) {
        if (is_first) {
            is_first = false;
            is_last_loaded = (*iter)->get_loaded();
            ++iter;
        } else {
            if (is_last_loaded == (*iter)->get_loaded()) {
                self_type::iterator bite = iter;
                --bite;
                (*bite)->set_bytes((*bite)->get_bytes() + (*iter)->get_bytes());
                delete *iter;
                iter = _pages.erase(iter);
            } else {
                is_last_loaded = (*iter)->get_loaded();
                ++iter;
            }
        }
    }

    return true;
}

bool ObjectPageList::parse(off_t new_pos)
{
    for (self_type::iterator iter = _pages.begin(); iter != _pages.end(); ++iter) {
        if (new_pos == (*iter)->get_offset()) {
            return true;
        } else if ((*iter)->get_offset() < new_pos && new_pos < (*iter)->next()) {
            ObjectPage *page = new ObjectPage((*iter)->get_offset(),
                    static_cast<size_t>(new_pos - (*iter)->get_offset()),
                    (*iter)->get_loaded());
            (*iter)->set_bytes((*iter)->get_bytes() - new_pos + (*iter)->get_offset());
            (*iter)->set_offset(new_pos);
            _pages.insert(iter, page);
            return true;
        }
    }
    return false;
}

// Definition of class StatCacheFile
StatCacheFile::StatCacheFile(DataCache *data_cache, const char *path)
    : _data_cache(data_cache), _path(""), _fd(-1) {
    if (path && '\0' != path[0]) {
        set_path(path, true);
    }
}

StatCacheFile::~StatCacheFile()
{
    release();
    _data_cache = nullptr;
}

bool StatCacheFile::open_file()
{
    if (0 == _path.size()) {
        return false;
    }
    if (-1 != _fd) {
        return true;
    }

    std::string stat_file;
    if (!_data_cache->make_path(_path.c_str(), stat_file, true)) {
        BOSFS_ERR("failed to create stat cache file path(%s)", _path.c_str());
        return false;
    }

    if (-1 == (_fd = open(stat_file.c_str(), O_CREAT|O_RDWR, 0600))) {
        BOSFS_ERR("failed to open stat cache file path(%s) - errno(%d)", _path.c_str(), errno);
        return false;
    }

    if (-1 == flock(_fd, LOCK_EX)) {
        BOSFS_ERR("failed to lock stat cache file path(%s) - errno(%d)", _path.c_str(), errno);
        flock(_fd, LOCK_UN);
        close(_fd);
        _fd = -1;
        return false;
    }

    if (0 != lseek(_fd, 0, SEEK_SET)) {
        BOSFS_ERR("failed to lseek stat cache file path(%s) - errno(%d)", _path.c_str(), errno);
        flock(_fd, LOCK_UN);
        close(_fd);
        _fd = -1;
        return false;
    }
    BOSFS_DEBUG("file locked (%s - %s)", _path.c_str(), stat_file.c_str());
    return true;
}

bool StatCacheFile::release()
{
    if (-1 == _fd) {
        return true;
    }

    if (-1 == flock(_fd, LOCK_UN)) {
        BOSFS_ERR("failed to unlock stat cache file path(%s) - errno(%d)", _path.c_str(), errno);
        return false;
    }
    BOSFS_DEBUG("file unlocked (%s)", _path.c_str());

    if (-1 == close(_fd)) {
        BOSFS_ERR("failed to close stat cache file path(%s) - errno(%d)", _path.c_str(), errno);
        return false;
    }
    _fd = -1;
    return true;
}

bool StatCacheFile::set_path(const char *path, bool is_open)
{
    if (!path || '\0' == path[0]) {
        return false;
    }

    if (!release()) {
        return false;
    }

    if (path) {
        _path = path;
    }
    if (!is_open) {
        return true;
    }
    return open_file();
}

bool DataCache::make_path(const char *path, std::string &file_path, bool is_create_dir)
{
    // Make stat cache path: /<cache_path>/.<bucket_name>.stat
    std::string top_path = get_cache_dir();
    top_path += "/." + _bosfs_util->options().bucket + ".stat";

    if (is_create_dir) {
        int ret = 0;
        std::string dir = top_path + path;
        dir.resize(dir.rfind('/'));
        if (0 != (ret = SysUtil::mkdirp(dir, 0777))) {
            BOSFS_ERR("failed to create dir(%s), errno(%d)", path, ret);
            return false;
        }
    }
    if (!path || '\0' == path[0]) {
        file_path = top_path;
    } else {
        file_path = top_path + path;
    }
    return true;
}

// Definition of class DataCacheEntity
DataCacheEntity::DataCacheEntity(BosfsUtil *bosfs_util, DataCache *data_cache, FileManager *file_manager,
    const char *tpath, const char *cpath)
    : _bosfs_util(bosfs_util), _data_cache(data_cache), _file_manager(file_manager),
      _ref_count(0), _path(""), _cache_path(""), _mirror_path(""), _fd(-1),
      _is_modified(false), _origin_meta_size(0), _upload_id(""), _mp_start(0), _mp_size(0),
      _is_tmpfile(false) {
    _path = tpath ? tpath : "";
    _cache_path = cpath ? cpath : "";

    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&_entity_lock, &attr);
}

DataCacheEntity::~DataCacheEntity() {
    clear();
    pthread_mutex_destroy(&_entity_lock);
    _bosfs_util = nullptr;
    _data_cache = nullptr;
    _file_manager = nullptr;
}

bool DataCacheEntity::is_safe_disk_space(size_t size) {
    const char *local_file;
    if (_is_tmpfile) {
        local_file = _data_cache->tmp_dir_cstr();
    } else {
        local_file = _data_cache->get_cache_dir();
    }
    struct statvfs st;
    int ret = statvfs(local_file, &st);
    if (ret != 0) {
        BOSFS_ERR("could not statvfs %s, errno(%d)", local_file, errno);
        return false;
    }
    size_t reserved_space = _data_cache->get_ensure_free_disk_space();
    return (size + reserved_space) <= st.f_bavail * st.f_bsize;
}

int DataCacheEntity::close_file()
{
    BOSFS_DEBUG("[path=%s][fd=%d][refcount=%d]", _path.c_str(), _fd, _ref_count);
    if (_fd < 0) {
        BOSFS_WARN("double close file:%s, refcount:%d", _path.c_str(), _ref_count);
        return 0;
    }

    AutoLock auto_lock(&_entity_lock);
    if (_ref_count <= 0) {
        BOSFS_WARN("double dereference file:%s, refcount:%d", _path.c_str(), _ref_count);
        return 0;
    }
    if (0 < --_ref_count) {
        return 0;
    }
    BOSFS_DEBUG("real close file %s, close local fd:%d refcount:%d", _path.c_str(), _fd, _ref_count);

    if (_is_tmpfile) {
        // tmpfile cache will be removed after fd close;
        //  so it must be flushed to bos right now
        int ret = flush();
        if (ret != 0) {
            BOSFS_ERR("flush before close failed, error: %d", -ret);
            return ret;
        }
        if (unlink(_tmp_filename.c_str()) != 0) {
            BOSFS_ERR("unlink tmp file:%s failed, errno:%d", _tmp_filename.c_str(), errno);
            return -errno;
        }
        _tmp_filename.clear();
    }
    if (0 != _cache_path.size()) {
        StatCacheFile stat_cache(_data_cache, _path.c_str());
        if (!_page_list.serialize(stat_cache, true)) {
            BOSFS_WARN("failed to save stat cache file (%s)", _path.c_str());
        }
    }
    close(_fd);
    _fd = -1;

    if (!_mirror_path.empty()) {
        if (-1 == unlink(_mirror_path.c_str())) {
            BOSFS_WARN("failed to remove mirror cache file(%s) by errno(%d)",
                    _mirror_path.c_str(), errno);
            return -errno;
        }
        _mirror_path.erase();
    }
    return 0;
}

int DataCacheEntity::truncate(off_t size) {
    if (_fd < 0) {
        return -EBADF;
    }
    if (-1 == ftruncate(_fd, size)) {
        BOSFS_ERR("failed to truncate temporary file(%d) by errno(%d).", _fd, errno);
        return -EIO;
    }
    // resize page list
    if (!_page_list.resize(size, false)) {
        BOSFS_ERR("failed to truncate temporary file information(%d).", _fd);
        return -EIO;
    }
    return 0;
}

int DataCacheEntity::open_file(ObjectMetaData *pmeta, ssize_t size, time_t time) {
    BOSFS_DEBUG("[path=%s][fd=%d][size=%jd][time=%jd]", _path.c_str(), _fd,
            (intmax_t)size, (intmax_t)time);

    if (-1 != _fd) {
        // already opened, needs to increment refcnt.
        dup_file();
        return 0;
    }

    bool  need_save_csf = false;  // need to save(reset) cache stat file
    bool  is_truncate   = false;  // need to truncate

    if (0 != _cache_path.size()) {
        // using cache

        // open cache and cache stat file, load page info.
        StatCacheFile cfstat(_data_cache, _path.c_str());

        // try to open cache file
        if (-1 != (_fd = open(_cache_path.c_str(), O_RDWR)) && _page_list.serialize(cfstat, false)) {
            // succeed to open cache file and to load stats data
            struct stat st;
            memset(&st, 0, sizeof(struct stat));
            if (-1 == fstat(_fd, &st)) {
                BOSFS_ERR("fstat is failed. errno(%d)", errno);
                _fd = -1;
                return (0 == errno ? -EIO : -errno);
            }
            // check size, st_size, loading stat file
            if (-1 == size) {
                if (static_cast<size_t>(st.st_size) != _page_list.get_size()) {
                    _page_list.resize(st.st_size, false);
                    need_save_csf = true;     // need to update page info
                }
                size = static_cast<ssize_t>(st.st_size);
            } else {
                if (static_cast<size_t>(size) != _page_list.get_size()) {
                    _page_list.resize(static_cast<size_t>(size), false);
                    need_save_csf = true;     // need to update page info
                }
                if (static_cast<size_t>(size) != static_cast<size_t>(st.st_size)) {
                    is_truncate = true;
                }
            }
        }else{
            // could not open cache file or could not load stats data, so initialize it.
            if (-1 == (_fd = open(_cache_path.c_str(), O_CREAT|O_RDWR|O_TRUNC, 0600))) {
                BOSFS_ERR("failed to open file(%s). errno(%d)", _cache_path.c_str(), errno);
                return (0 == errno ? -EIO : -errno);
            }
            need_save_csf = true;       // need to update page info
            if (-1 == size) {
                size = 0;
                _page_list.init(0, false);
            } else {
                _page_list.resize(static_cast<size_t>(size), false);
                is_truncate = true;
            }
        }

        // open mirror file
        int mirrorfd;
        if (0 >= (mirrorfd = open_mirror_file())) {
            BOSFS_ERR("failed to open mirror file linked cache file(%s).", _cache_path.c_str());
            return (0 == mirrorfd ? -EIO : mirrorfd);
        }
        // switch fd
        close(_fd);
        _fd = mirrorfd;
    } else { // not using cache
        // open temporary file
        if (_tmp_filename.size() == 0) {
            // generate uuid for tmp file name because file name may be to long for system
            uuid_t out;
            uuid_generate(out);
            _tmp_filename.resize(36);
            uuid_unparse(out, const_cast<char *>(_tmp_filename.data()));
            std::transform(_tmp_filename.begin(), _tmp_filename.end(), _tmp_filename.begin(), tolower);
            _tmp_filename = _data_cache->tmp_dir() + "/bosfs.tmp." + _tmp_filename;
        }
        _fd = open(_tmp_filename.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0600);
        if (_fd < 0) {
            BOSFS_ERR("failed to open file:%s, errno:%d", _tmp_filename.c_str(), errno);
            return (0 == errno ? -EIO : -errno);
        }
        BOSFS_INFO("use tmp file:%s map to %s", _tmp_filename.c_str(), _path.c_str());
        if (-1 == size) {
            size = 0;
            _page_list.init(0, false);
        } else {
            _page_list.resize(static_cast<size_t>(size), false);
            is_truncate = true;
        }
        _is_tmpfile = true;
    }

    // truncate cache(tmp) file
    if (is_truncate) {
        if (0 != ftruncate(_fd, static_cast<off_t>(size)) || 0 != fsync(_fd)) {
            BOSFS_ERR("ftruncate(%s) or fsync returned err(%d)", _cache_path.c_str(), errno);
            _fd = -1;
            return (0 == errno ? -EIO : -errno);
        }
    }

    // reset cache stat file
    if (need_save_csf) {
        StatCacheFile cfstat(_data_cache, _path.c_str());
        if (!_page_list.serialize(cfstat, true)) {
            BOSFS_WARN("failed to save cache stat file(%s), but continue...", _path.c_str());
        }
    }

    // init internal data
    _ref_count = 1;
    _is_modified = false;

    // set original headers and size in it.
    if (pmeta) {
        _origin_meta.copy_from(*pmeta);
        _origin_meta_size = _origin_meta.content_length();
    } else {
        _origin_meta.clear();
        _origin_meta_size = 0;
    }
    if (!_bosfs_util->options().storage_class.empty()) {
        _origin_meta.set_storage_class(_bosfs_util->options().storage_class);
    }

    // set mtime(set "x-bce-meta-mtime")
    if (-1 != time) {
        if (0 != set_mtime(time)) {
            BOSFS_ERR("failed to set mtime. errno(%d)", errno);
            _fd = -1;
            return (0 == errno ? -EIO : -errno);
        }
    }

    return 0;
}

bool DataCacheEntity::open_and_load_all(ObjectMetaData *pmeta, size_t *size, bool force_load)
{
    int result = 0;
    BOSFS_INFO("[path=%s][fd=%d]", _path.c_str(), _fd);

    if (-1 == _fd) {
        if (0 != open_file(pmeta)) {
            return false;
        }
    }

    AutoLock auto_lock(&_entity_lock);
    if (force_load) {
        set_all_status_unloaded();
    }

    if (0 != (result = load())) {
        BOSFS_ERR("could not download, result(%d)", result);
        return false;
    }
    if (_is_modified) {
        _is_modified = false;
    }
    if (size) {
        *size = _page_list.get_size();
    }
    return true;
}

int DataCacheEntity::dup_file()
{
    BOSFS_DEBUG("[path=%s][fd=%d][refcount=%d]", _path.c_str(), _fd,
            (-1 != _fd ? _ref_count + 1 : _ref_count));
    if (-1 != _fd) {
        AutoLock auto_lock(&_entity_lock);
        _ref_count++;
    }
    return _fd;
}

bool DataCacheEntity::get_stats(struct stat &st)
{
    if (-1 == _fd) {
        return false;
    }

    AutoLock auto_lock(&_entity_lock);
    memset(&st, 0, sizeof(struct stat));
    if (-1 == fstat(_fd, &st)) {
        BOSFS_ERR("fstat failed, errno(%d)", errno);
        return false;
    }
    return true;
}

int DataCacheEntity::set_mtime(time_t time)
{
    BOSFS_INFO("[path=%s][fd=%d][time=%d]", _path.c_str(), _fd, (intmax_t)time);
    AutoLock auto_lock(&_entity_lock);

    if (-1 == time) {
        return 0;
    }
    if (-1 != _fd) {
        AutoLock auto_lock(&_entity_lock);

        struct timeval tv[2];
        tv[0].tv_sec = time;
        tv[0].tv_usec = 0L;
        tv[1].tv_sec = tv[0].tv_sec;
        tv[1].tv_usec = 0L;
        if (-1 == futimes(_fd, tv)) {
            BOSFS_ERR("futimes failed, errno(%d)", errno);
            return -errno;
        }
    } else if (0 < _cache_path.size()) {
        struct utimbuf n_mtime;
        n_mtime.modtime = time;
        n_mtime.actime = time;
        if (-1 == utime(_cache_path.c_str(), &n_mtime)) {
            BOSFS_ERR("utime failed, errno(%d)", errno);
            return -errno;
        }
    }

    _origin_meta.set_user_meta("bosfs-mtime", time);
    return 0;
}

bool DataCacheEntity::update_mtime()
{
    AutoLock auto_lock(&_entity_lock);
    struct stat st;
    if (!get_stats(st)) {
        return false;
    }

    _origin_meta.set_user_meta("bosfs-mtime", st.st_mtime);
    return true;
}

bool DataCacheEntity::get_size(size_t &size)
{
    if (-1 == _fd) {
        return false;
    }

    AutoLock auto_lock(&_entity_lock);
    size = _page_list.get_size();
    return true;
}

bool DataCacheEntity::set_mode(mode_t mode)
{
    AutoLock auto_lock(&_entity_lock);
    _origin_meta.set_user_meta("bosfs-mode", mode);
    return true;
}

bool DataCacheEntity::set_uid(uid_t uid)
{
    AutoLock auto_lock(&_entity_lock);
    _origin_meta.set_user_meta("bosfs-uid", uid);
    return true;
}

bool DataCacheEntity::set_gid(gid_t gid)
{
    AutoLock auto_lock(&_entity_lock);
    _origin_meta.set_user_meta("bosfs-gid", gid);
    return true;
}

void DataCacheEntity::set_xattr(const std::string &xattr) {
    AutoLock auto_lock(&_entity_lock);
    _origin_meta.set_user_meta("bosfs-xattr", xattr);
}

int DataCacheEntity::load(off_t start, size_t size)
{
    BOSFS_DEBUG("[path=%s][fd=%d][offset=%jd][size=%jd]", _path.c_str(), _fd,
            (intmax_t)start, (intmax_t)size);
    if (-1 == _fd) {
        return -EBADF;
    }
    if (size == 0) {
        return 0;
    }

    AutoLock auto_lock(&_entity_lock);
    int result = 0;
    ObjectPageList::self_type unloaded_list;
    _page_list.get_unloaded_pages(unloaded_list, start, size);
    if (unloaded_list.empty()) {
        return 0;
    }
    off_t end = start + size;
    for (ObjectPageList::self_type::iterator iter = unloaded_list.begin();
            iter != unloaded_list.end(); ++iter) {
        if (end <= (*iter)->get_offset()) {
            break;
        }
        ObjectPage *page = *iter;

        size_t need_load_size = 0;
        size_t over_size = 0;
        if ((off_t) _origin_meta_size > page->get_offset()) {
            if ((off_t) _origin_meta_size >= page->next()) {
                need_load_size = page->get_bytes();
            } else {
                need_load_size = _origin_meta_size - page->get_offset();
                over_size = page->next() - _origin_meta_size;
            }
        }

        if (0 < need_load_size) {
            BOSFS_INFO("unloaded page off: %ld, size: %ld, need_load: %u, origin: %ld",
                    page->get_offset(), page->get_bytes(), need_load_size, _origin_meta_size);
            result = _bosfs_util->bos_client()->parallel_download(_bosfs_util->options().bucket, _path, _fd,
                    page->get_offset(), need_load_size);
            if (0 != result) {
                break;
            }
        }
        if (0 < over_size) {
            result = fill_file(_fd, 0, over_size, (*iter)->get_offset() + need_load_size);
            if (result != 0) {
                BOSFS_ERR("failed to fill rest bytes for fd(%d), errno(%d)", _fd, result);
                break;
            }
            _is_modified = false;
        }

        _page_list.set_page_loaded_status((*iter)->get_offset(), (*iter)->get_bytes(), true);
    }
    ObjectPageList::free_list(unloaded_list);
    return result;
}

int DataCacheEntity::row_flush(const char *tpath, bool force_sync)
{
    if (-1 == _fd) {
        return -EBADF;
    }
    AutoLock auto_lock(&_entity_lock);

    if (!force_sync && !_is_modified) { // nothing to update
        return 0;
    }

    int ret = 0;
    size_t rest_size = _page_list.get_total_unloaded_page_size();
    if (rest_size > 0) {
        if (is_safe_disk_space(rest_size)) {
            if (0 != (ret = load())) {
                BOSFS_ERR("%s", "failed to load all area");
                return static_cast<ssize_t>(ret);
            }
        } else {
            BOSFS_ERR("%s", "not enough disk space");
            return -1;
        }
    }
    if (lseek(_fd, 0, SEEK_SET) < 0) {
        BOSFS_ERR("seek file(%d) to file head failed: %d", _fd, errno);
        return -errno;
    }
    std::string object_name = tpath != NULL ? tpath + 1 : _path.substr(1);
    if ((int64_t) _page_list.get_size() < _bosfs_util->options().multipart_threshold) {
        ret = _bosfs_util->bos_client()->upload_file(_bosfs_util->options().bucket, object_name, _fd, &_origin_meta);
    } else {
        ret = _bosfs_util->bos_client()->upload_super_file(_bosfs_util->options().bucket, object_name, _fd, &_origin_meta);
    }
    if (ret != 0) {
        BOSFS_ERR("failed to upload to bos from file(%d)", _fd);
        return -1;
    }
    _is_modified = false;
    _file_manager->del(_path);
    return 0;
}

int DataCacheEntity::flush(bool force_sync)
{
    return row_flush(NULL, force_sync);
}

ssize_t DataCacheEntity::read(char *bytes, off_t start, size_t size, bool force_load)
{
    if (-1 == _fd) {
        return -EBADF;
    }
    if (size == 0) {
        return 0;
    }
    AutoLock auto_lock(&_entity_lock);
    if (force_load) {
        _page_list.set_page_loaded_status(start, size, false);
    }

    // Check disk space
    int ret = 0;
    if (0 < _page_list.get_total_unloaded_page_size(start, size)) {
        if (!is_safe_disk_space(size)) {
            if (!_is_modified) {
                _page_list.init(_page_list.get_size(), false);
                // free blocks on disk
                if (-1 == ftruncate(_fd, 0) || -1 == ftruncate(_fd, _page_list.get_size())) {
                    BOSFS_ERR("failed to truncate temporary file %d", _fd);
                    return -ENOSPC;
                }
            }
        }

        // Prefetch load size
        size_t load_size = size;
        if (static_cast<size_t>(start + size) < _page_list.get_size()) {
            // maximum load size if won't cost more time
            size_t prefetch_max_size = std::max(size,
                    static_cast<size_t>(_bosfs_util->options().multipart_size * _bosfs_util->options().multipart_parallel));
            if (static_cast<size_t>(start + prefetch_max_size) < _page_list.get_size()) {
                load_size = prefetch_max_size;
            } else {
                load_size = static_cast<size_t>(_page_list.get_size() - start);
            }
        }

        // Loading
        ret = load(start, load_size);
        if (ret != 0) {
            BOSFS_ERR("could not download, start(%jd), size(%zu), errno(%d)",
                    static_cast<intmax_t>(start), size, ret);
            return -EIO;
        }
    }

    // Do reading from local data cache file
    ssize_t rsize = 0;
    if (-1 == (rsize = pread(_fd, bytes, size, start))) {
        BOSFS_ERR("pread failed, errno(%d)", errno);
        return -errno;
    }
    return rsize;
}

ssize_t DataCacheEntity::write(const char *bytes, off_t start, size_t size)
{
    if (-1 == _fd) {
        return -EBADF;
    }
    AutoLock auto_lock(&_entity_lock);
    int ret = 0;

    // Check file size
    size_t cur_size = _page_list.get_size();
    if (cur_size < static_cast<size_t>(start)) {
        if (-1 == ftruncate(_fd, static_cast<size_t>(start))) {
            BOSFS_ERR("failed to truncate temporary file %d", _fd);
            return -EIO;
        }

        _page_list.set_page_loaded_status(static_cast<off_t>(cur_size),
                static_cast<size_t>(start) - cur_size, false);
    }

    // Load uninitialized area from 0 to start
    size_t rest_size = _page_list.get_total_unloaded_page_size(0, start) + size;
    if (is_safe_disk_space(rest_size)) {//是否有足够一次multi_upload(reserved)+rest_size的disk大小
        if (start > 0 && 0 != (ret = load(0, static_cast<size_t>(start)))) {
            BOSFS_ERR("failed to load uninitialized area before writing(errno=%d)", ret);
            return -EIO;
        }
    } else {
        BOSFS_ERR("not enough disk space for writing");
        return -ENOSPC;
    }
    BOSFS_DEBUG("write to fd: %d, off: %ld, size: %ld", _fd, start, size);

    // Do writing from start to start + size
    ssize_t write_size = -1;
    if (-1 == (write_size = pwrite(_fd, bytes, size, start))) {
        BOSFS_ERR("pwrite failed, errno=%d", errno);
        return -errno;
    }

    if (write_size > 0) {
        _is_modified = true;
        _page_list.set_page_loaded_status(start, static_cast<size_t>(write_size), true);
    }
    return write_size;
}

int DataCacheEntity::fill_file(int fd, unsigned char byte, size_t size, off_t start)
{/*{{{*/
    unsigned char bytes[32 * 1024];
    memset(bytes, byte, sizeof(bytes) > size ? size : sizeof(bytes));

    for (ssize_t total = 0, onewrote = 0; static_cast<size_t>(total) < size; total += onewrote) {
        if (-1 == (onewrote = pwrite(fd, bytes,
                        std::min(sizeof(bytes), size - static_cast<size_t>(total)),
                        start + total))) {
            BOSFS_ERR("pwrite failed, errno: %d", errno);
            return -errno;
        }
    }
    return 0;
}/*}}}*/

void DataCacheEntity::clear()
{/*{{{*/
    AutoLock auto_lock(&_entity_lock);
    if (_fd >= 0) {
        BOSFS_WARN("try clear all, but local file still open, close fd:%d", _fd);
        if (0 != _cache_path.size()) {
            StatCacheFile statcf(_data_cache, _path.c_str());
            if (!_page_list.serialize(statcf, true)) {
                BOSFS_WARN("failed to save stat cache to file (%s)", _path.c_str());
            }
        }
        close(_fd);
        _fd = -1;
        if (!_mirror_path.empty()) {
            if (-1 == unlink(_mirror_path.c_str())) {
                BOSFS_WARN("failed to remove mirror cache file (%s), by errno(%d)",
                        _mirror_path.c_str(), errno);
            }
            _mirror_path.clear();
        }
        if (!_tmp_filename.empty()) {
            if (unlink(_tmp_filename.c_str()) != 0) {
                BOSFS_ERR("unlink tmp file:%s failed, errno:%d", _tmp_filename.c_str(), errno);
            }
            _tmp_filename.clear();
        }
    }
    _page_list.init(0, false);
    _ref_count = 0;
    _path = "";
    _cache_path = "";
    _is_modified = false;
}/*}}}*/

int DataCacheEntity::open_mirror_file()
{/*{{{*/
    if (_cache_path.empty()) {
        BOSFS_ERR("%s", "cache path is empty");
        return -EIO;
    }

    std::string tmp_dir;
    if (!_data_cache->make_cache_path(NULL, tmp_dir, true, true)) {
        BOSFS_ERR("%s", "could not make cache directory path");
        return -EIO;
    }

    // generate random name for mirror file
    char mirror_file[NAME_MAX + 1] = {0};
    uuid_t out;
    uuid_generate(out);
    uuid_unparse(out, mirror_file);
    _mirror_path = tmp_dir + "/" + mirror_file;

    if (-1 == link(_cache_path.c_str(), _mirror_path.c_str())) {
        BOSFS_ERR("could not link mirror file(%s) to cache file(%s), errno: %d",
                _mirror_path.c_str(), _cache_path.c_str(), errno);
        return -errno;
    }

    int mirror_fd = -1;
    if (-1 == (mirror_fd = open(_mirror_path.c_str(), O_RDWR))) {
        BOSFS_ERR("could not open mirror file (%s), errno: %d", _mirror_path.c_str(), errno);
        return -errno;
    }
    return mirror_fd;
}/*}}}*/

bool DataCacheEntity::set_all_status(bool is_loaded)
{/*{{{*/
    BOSFS_INFO("[path=%s][fd=%d][%s]", _path.c_str(), _fd, is_loaded ? "loaded" : "unloaded");

    if (-1 == _fd) {
        return false;
    }

    struct stat st;
    memset(&st, 0, sizeof(struct stat));
    if (-1 == fstat(_fd, &st)) {
        BOSFS_ERR("fstat is failed, errno: %d", errno);
        return false;
    }

    _page_list.init(st.st_size, is_loaded);
    return true;
}/*}}}*/

// makes cache directory empty on fs startup
bool DataCache::delete_cache_dir()
{
    if (0 == _cache_dir.size()) {
        return true;
    }

    std::string cache_dir;
    if (!make_cache_path(NULL, cache_dir, false)) {//获取bucket路径
        return false;
    }
    return SysUtil::delete_files_in_dir(cache_dir.c_str(), true);
}

int DataCache::delete_cache_file(const char *path)
{
    BOSFS_INFO("[path=%s]", path ? path : "");

    if (!path) {
        return -EIO;
    }
    if (0 == _cache_dir.size()) {
        return 0;
    }

    std::string cache_path = "";
    if (!make_cache_path(path, cache_path, false)) {
        return 0;
    }

    int ret = 0;
    if (0 != unlink(cache_path.c_str())) {
        if (ENOENT == errno) {
            BOSFS_DEBUG("failed to delete file(%s): errno=%d", path, errno);
        } else {
            BOSFS_ERR("failed to delete file(%s): errno=%d", path, errno);
        }
        ret = -errno;
    }

    if (!delete_file(path)) {
        if (ENOENT == errno) {
            BOSFS_DEBUG("failed to delete file(%s): errno=%d", path, errno);
        } else {
            BOSFS_ERR("failed to delete file(%s): errno=%d", path, errno);
        }
        if (0 != errno) {
            ret = -errno;
        } else {
            ret = -EIO;
        }
    }
    return ret;
}

int DataCache::set_cache_dir(const std::string &dir) {
    struct stat st;
    if (0 != stat(dir.c_str(), &st)) {
        BOSFS_ERR("could not access cache directory(%s), errno(%d)", dir.c_str(), errno);
        return errno;
    }
    if (!S_ISDIR(st.st_mode)) {
        BOSFS_ERR("the cache directory(%s) is not a directory", dir.c_str());
        return -ENOTDIR;
    }
    _cache_dir = dir;
    return 0;
}

bool DataCache::make_cache_path(const char *path, std::string &cache_path,
        bool is_create_dir, bool is_mirror_path)
{
    if (_cache_dir.empty()) {
        return true;
    }

    std::string path_to_make(_cache_dir);
    if (!is_mirror_path) {
        path_to_make += "/";
        path_to_make += _bosfs_util->options().bucket;
    } else {
        path_to_make += "/";
        path_to_make += _bosfs_util->options().bucket;
        path_to_make += ".mirror";
    }

    if (is_create_dir) {
        int ret = 0;
        std::string dir = path_to_make;
        if (path != NULL) {
            dir += path;
            dir.resize(dir.rfind('/'));
        }
        if (0 != (ret = SysUtil::mkdirp(dir, 0777))) {
            BOSFS_ERR("failed to create dir(%s), errno(%d)", dir.c_str(), ret);
            return false;
        }
    }

    if (!path || '\0' == path[0]) {
        cache_path = path_to_make;
    } else {
        cache_path = path_to_make + path;
    }
    return true;
}

bool DataCache::check_cache_top_dir()
{
    if (0 == _cache_dir.size()) {
        return true;
    }
    std::string top_path(_cache_dir + "/" + _bosfs_util->options().bucket);
    return SysUtil::check_exist_dir_permission(top_path.c_str());
}

size_t DataCache::set_ensure_free_disk_space(size_t size)
{
    const BosfsOptions &bosfs_options = _bosfs_util->options();
    size_t old_size = _free_disk_space;
    if (0 == size) {
        if (0 == _free_disk_space) {
            _free_disk_space = static_cast<size_t>(bosfs_options.multipart_size * bosfs_options.multipart_parallel);
        }
    } else {
        if (0 == _free_disk_space) {
            _free_disk_space = std::max(size,
                static_cast<size_t>(bosfs_options.multipart_size * bosfs_options.multipart_parallel));
        } else {
            if (static_cast<size_t>(bosfs_options.multipart_size * bosfs_options.multipart_parallel) <= size) {
                _free_disk_space = size;
            }
        }
    }
    return old_size;
}

DataCache::DataCache(BosfsUtil *bosfs_util, FileManager *file_manager)
    : _bosfs_util(bosfs_util), _file_manager(file_manager), _free_disk_space(0) {
    pthread_mutex_init(&_data_cache_lock, NULL);
}

DataCache::~DataCache() {
    for (DataCacheMap::iterator it = _data_cache.begin(); it != _data_cache.end(); ++it) {
        delete it->second;
    }
    pthread_mutex_destroy(&_data_cache_lock);
    _bosfs_util = nullptr;
    _file_manager = nullptr;
}

DataCacheEntity *DataCache::get_cache(const char *path) {
    AutoLock auto_lock(&_data_cache_lock);
    DataCacheMap::iterator it = _data_cache.find(path);
    if (it != _data_cache.end()) {
        return it->second;
    }
    return NULL;
}

DataCacheEntity * DataCache::open_cache(const char *path, ObjectMetaData *pmeta, ssize_t size,
        time_t time, bool force_tmpfile, bool is_create) {
    BOSFS_DEBUG("[path=%s][size=%jd][time=%jd]", path ? path : "", (intmax_t)size, (intmax_t)time);
    AutoLock auto_lock(&_data_cache_lock);
    DataCacheMap::iterator iter = _data_cache.find(std::string(path));
    DataCacheEntity * ent;
    if (_data_cache.end() != iter) {
        ent = iter->second;
    } else if (is_create) {
        std::string cache_path = "";
        if (!force_tmpfile && !DataCache::make_cache_path(path, cache_path, true)) {
            BOSFS_ERR("failed to make cache path for object (%s)", path);
            return NULL;
        }
        ent = new DataCacheEntity(_bosfs_util, this, _file_manager, path, cache_path.c_str());
        _data_cache[std::string(path)] = ent;
    } else {
        return NULL;
    }

    // open data cache entity
    if (-1 == ent->open_file(pmeta, size, time)) {//TODO:open_file will never return -1
        return NULL;
    }
    return ent;
}

DataCacheEntity *DataCache::exist_open(const char *path) {
    return open_cache(path, NULL, -1, -1, false, false);
}

bool DataCache::close_cache(DataCacheEntity *ent) {
    BOSFS_DEBUG("[ent->file=%s][ent->fd=%d]", ent ? ent->get_path() : "",
            ent ? ent->get_fd() : -1);

    AutoLock auto_lock(&_data_cache_lock);
    DataCacheMap::iterator it = _data_cache.find(ent->get_path());
    if (it == _data_cache.end()) {
        // bug tolerance
        for (it = _data_cache.begin(); it != _data_cache.end(); ++it) {
            if (it->second == ent) {
                break;
            }
        }
    }
    if (it == _data_cache.end()) {
        return false;
    }
    ent->close_file();
    if (!ent->is_open()) {
        _data_cache.erase(it);
        delete ent;
        return true;
    }
    return false;
}

bool DataCache::delete_file(const char *path)
{
    if (!path || '\0' == path[0]) {
        return false;
    }

    std::string sfile;
    if (!make_path(path, sfile, false)) {
        BOSFS_ERR("failed to create stat cache file path (%s)", path);
        return false;
    }
    if (0 != unlink(sfile.c_str())) {
        if (ENOENT == errno) {
            BOSFS_DEBUG("failed to delete file(%s): errno = %d", path, errno);
        } else {
            BOSFS_ERR("failed to delete file(%s): errno = %d", path, errno);
        }
        return false;
    }
    return true;
}

bool DataCache::check_top_dir()
{
    if (!is_cache_dir()) {
        return true;
    }

    std::string top_path = get_cache_dir();
    top_path += "/.";
    top_path += _bosfs_util->options().bucket;
    top_path += ".stat";

    return SysUtil::check_exist_dir_permission(top_path.c_str());
}

bool DataCache::delete_dir()
{
    std::string top_path = get_cache_dir();

    if (top_path.empty() || _bosfs_util->options().bucket.empty()) {
        return true;
    }
    top_path += "/.";
    top_path += _bosfs_util->options().bucket;
    top_path += ".stat";

    return SysUtil::delete_files_in_dir(top_path.c_str(), true);
}

END_FS_NAMESPACE

