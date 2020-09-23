/**
 * bosfs - A fuse-based file system implemented on Baidu Object Storage(BOS)
 *
 * Copyright (c) 2016 Baidu.com, Inc. All rights reserved.
 *
 * @file    bosfs_util.cpp
 * @author  Song Shuangyang(songshuangyang@baidu.com)
 * @date    2016.9
 **/
#include "bosfs_lib/bosfs_lib.h"
#include "bosfs_util.h"
#include "common.h"
#include "sys_util.h"
#include "file_manager.h"
#include <stdlib.h>    /* for malloc/free */
#include <unistd.h>    /* for sysconf, geteuid... */
#include <errno.h>     /* for error number */
#include <libgen.h>    /* for dirname/basename */
#include <pwd.h>      /* for passwd */
#include <grp.h>      /* for group */
#include <dirent.h>
#include <fstream>
#include <sstream>
#include <vector>
#include <pthread.h>

#include <cstdio>

BEGIN_FS_NAMESPACE

using namespace baidu::bos::cppsdk;

BosfsUtil::BosfsUtil()
    : _file_manager(nullptr), _data_cache(nullptr) {
    pthread_mutex_init(&_client_mutex, NULL);
}

BosfsUtil::~BosfsUtil() {
    _file_manager = nullptr;
    _data_cache = nullptr;
}

struct fuse_context *BosfsUtil::fuse_get_context() {
    if (!options().mock_fuse_calls) {
        return ::fuse_get_context();
    } else {
        static struct fuse_context s_fuse_context;
        s_fuse_context.uid = geteuid();
        s_fuse_context.gid = getegid();
        return &s_fuse_context;
    }
}

void BosfsUtil::set_file_manager(FileManager *file_manager) {
    _file_manager = file_manager;
}

void BosfsUtil::set_data_cache(DataCache *data_cache) {
    _data_cache = data_cache;
}

SharedPtr<Client> BosfsUtil::bos_client() {
    MutexGuard lock(&_client_mutex);
    return _bos_client;
}

std::string BosfsUtil::get_real_path(const char* path) {
    std::string realpath = "/" + options().bucket_prefix + std::string(path+1);
    if (*realpath.rbegin() == '/' && realpath != "/") {
        realpath = realpath.substr(0, realpath.size()-1); 
    }
    return realpath;
}

int BosfsUtil::_parse_bosfs_options(BosfsOptions &bosfs_options, std::string &errmsg) {
    if (bosfs_options.bucket_prefix != "") {
        if (*bosfs_options.bucket_prefix.rbegin() != '/') {
            bosfs_options.bucket_prefix.push_back('/');
        }
    }

    // validate mounted bucket
    if (bosfs_options.bucket.size() == 0) {
        return return_with_error_msg(errmsg, "missing BUCKET argument");
    }
    if (bosfs_options.bucket.find_first_of("/:\\;!@#$%^&*?") != std::string::npos) {
        return return_with_error_msg(errmsg, "bucket name(%s) contains illegal letter", bosfs_options.bucket.c_str());
    }

    // setup mntpoint attr
    bosfs_options.mount_uid = geteuid();
    bosfs_options.mount_gid = getegid();
    bosfs_options.mount_mode = S_IFDIR | (bosfs_options.allow_other ? (
            ~bosfs_options.mount_umask & (S_IRWXU | S_IRWXG | S_IRWXO)) : S_IRWXU);
    bosfs_options.mount_time = time(NULL);

    if (!bosfs_options.cache_dir.empty()) {
        if (SysUtil::check_local_dir("cache", bosfs_options.cache_dir, errmsg) != 0) {
            return -1;
        }
        int ret = _data_cache->set_cache_dir(bosfs_options.cache_dir);
        if (ret != 0) {
            return return_with_error_msg(errmsg, "set cache dir %s failed: %d", bosfs_options.cache_dir.c_str(), ret);
        }
    }

    if (bosfs_options.meta_expires_s > 0) {
        _file_manager->set_expire_s(bosfs_options.meta_expires_s);
    }
    if (bosfs_options.meta_capacity < 0) {
        bosfs_options.meta_capacity = 100000;
    }
    _file_manager->set_cache_capacity(bosfs_options.meta_capacity);

    if (!bosfs_options.storage_class.empty()) {
        if (bosfs_options.storage_class != "STANDARD" && bosfs_options.storage_class != "STANDARD_IA") {
            return return_with_error_msg(errmsg, "invalid storage class: %s", bosfs_options.storage_class);
        }
    }

    const char *tmp_name = "tmp";
    if (bosfs_options.tmp_dir.empty()) {
        tmp_name = "default tmp";
        bosfs_options.tmp_dir = "/tmp";
    }
    if (SysUtil::check_local_dir(tmp_name, bosfs_options.tmp_dir, errmsg) != 0) {
        return -1;
    }
    _data_cache->set_tmp_dir(bosfs_options.tmp_dir);

    return 0;
}

int BosfsUtil::init_bos(BosfsOptions &bosfs_options, std::string &errmsg) {
    if (_parse_bosfs_options(bosfs_options, errmsg) != 0) {
        return -1;
    }

    // done parsing options
    _bosfs_options = bosfs_options;

    int ret = 0;
    // init ak/sk
    if (options().ak.empty() || options().sk.empty()) {
        return return_with_error_msg(errmsg, "ak or sk not specified");
    }

    // init client instance
    ret = create_bos_client(errmsg);
    if (ret != 0) {
        return ret;
    }

    // check bucket existence
    ret = exist_bucket(errmsg);
    if (ret != 0) {
        if (options().create_bucket) {
            ret = create_bucket(errmsg);
        }
        if (ret != 0) {
            if (options().create_bucket) {
                errmsg = "create bucket failed";
            } else {
                errmsg = "bucket does not exist";
            }
            return ret;
        }
    }

    //check bucket_prefix exit
    bool is_dir_obj = false;
    bool is_prefix = false;
    std::string prefix = options().bucket_prefix;
    if (options().bucket_prefix != "") {
        if (*options().bucket_prefix.rbegin() == '/') {
           prefix = options().bucket_prefix.substr(0, options().bucket_prefix.size() - 1); 
        }
        ret = head_object(prefix, NULL, &is_dir_obj, &is_prefix);
        if (ret != 0) {
            return return_with_error_msg(errmsg, "bucket prefix %s does not exist", prefix.c_str());
        }
        if (!is_dir_obj && !is_prefix) {
            return return_with_error_msg(errmsg, "not mounting a directory");
        }
        return 0;
    }

    // check cache dir
    if (!_data_cache->check_cache_top_dir() || !_data_cache->check_top_dir()) {
        return return_with_error_msg(errmsg, "can't check permission of cache directory");
    }
    _data_cache->init_ensure_free_disk_space();
    
    // Delete cache dirs
    if (options().remove_cache) {
        if (!_data_cache->delete_dir()) {
            BOSFS_WARN("%s", "could not initialize cache directory");
        }
        if (!_data_cache->delete_cache_dir()) {
            BOSFS_WARN("%s", "could not initialize cache directory");
        }
    }

    // Initiate mime-types
    if (!SysUtil::init_mimetype()) {
        return return_with_error_msg(errmsg, "could not initiate mime-types");
    }

    return 0;
}

void BosfsUtil::init_default_stat(struct stat *pst) {
    memset(pst, 0, sizeof(struct stat));
    pst->st_nlink = 1;
    pst->st_mode = options().mount_mode;
    pst->st_uid = options().is_bosfs_uid ? options().bosfs_uid : options().mount_uid;
    pst->st_gid = options().is_bosfs_gid ? options().bosfs_gid : options().mount_gid;
    pst->st_ctime = options().mount_time;
    pst->st_mtime = options().mount_time;
    pst->st_size = 0;
    pst->st_blocks = 0;
    pst->st_blksize = ST_BLKSIZE;
}

std::string BosfsUtil::object_to_path(const std::string &object){
    if (object.empty()) {
        return "";
    }
    std::ostringstream result;
    if (object[0] != '/') {
        result << "/";
    }
    if (*object.rbegin() == '/') {
        result << object.substr(0, object.length() - 1);
    } else {
        result << object;
    }
    return result.str();
}

std::string BosfsUtil::object_to_basename(const std::string &object, const std::string &prefix) {
    std::string result;
    if (*object.rbegin() == '/') {
        result = object.substr(prefix.length(), object.length() - prefix.length() - 1);
    } else {
        result = object.substr(prefix.length(), object.length() - prefix.length());
    }
    return result;
}

int BosfsUtil::check_object_access(const char *path, int mask, struct stat *pstbuf)
{
    int ret = 0;

    struct fuse_context *pctx;
    if (NULL == (pctx = this->fuse_get_context())) {
        return -EIO;
    }

    struct stat st;
    struct stat *pst = pstbuf ? pstbuf : &st;
    ret = get_object_attribute(path, pst);
    if (ret != 0) {
        return ret;
    }

    if (0 == pctx->uid) { // root is allowed all accessing
        return 0;
    }

    if (options().is_bosfs_uid && options().bosfs_uid == pctx->uid) { // uid user is allowed
        return 0;
    }

    if (F_OK == mask) { // if there is a file return allowed
        return 0;
    }

    uid_t obj_uid = options().is_bosfs_uid ? options().bosfs_uid : pst->st_uid;
    gid_t obj_gid = options().is_bosfs_gid ? options().bosfs_gid : pst->st_gid;
    mode_t mode;
    mode_t base_mask = S_IRWXO;
    if (options().is_bosfs_umask) { // if umask is set, all object attr set ~umask
        mode = ((S_IRWXU | S_IRWXG | S_IRWXO) & ~options().bosfs_mask);
    } else {
        mode = pst->st_mode;
    }
    if (pctx->uid == obj_uid) {
        base_mask |= S_IRWXU;
    }
    if (pctx->gid == obj_gid) {
        base_mask |= S_IRWXG;
    }
    if (1 == SysUtil::is_uid_in_group(pctx->uid, obj_gid)) {
        base_mask |= S_IRWXG;
    }
    mode &= base_mask;

    if (X_OK == (mask & X_OK)) {
        if (0 == (mode & (S_IXUSR | S_IXGRP | S_IXOTH))) {
            return -EACCES;
        }
    }
    if (W_OK == (mask & W_OK)) {
        if (0 == (mode & (S_IWUSR | S_IWGRP | S_IWOTH))) {
            return -EACCES;
        }
    }
    if (R_OK == (mask & R_OK)) {
        if (0 == (mode & (S_IRUSR | S_IRGRP | S_IROTH))) {
            return -EACCES;
        }
    }
    if (0 == mode) {
        return -EACCES;
    }
    return 0;
}

int BosfsUtil::get_object_attribute(const std::string &path, struct stat *pstbuf,
        ObjectMetaData *pmeta) {
    struct stat tmpstbuf;
    struct stat *pst = pstbuf ? pstbuf : &tmpstbuf;
    init_default_stat(pst);
    if (path == "/" || path == ".") {
        pst->st_size = ST_BLKSIZE;
        pst->st_blocks = ST_MINBLOCKS;
        return 0;
    }

    FilePtr file;
    int ret = _file_manager->get(path, &file);
    if (ret != 0) {
        return ret;
    }
    if (pmeta != NULL) {
        *pmeta = file->meta();
    }
    if (pstbuf != NULL) {
        file->stat(pstbuf);
    }
    return 0;
}

// check each path component has searching permission
int BosfsUtil::check_path_accessible(const char *path) {
    std::string parent(path);
    size_t pos = parent.rfind('/');
    while (pos != 0) {
        parent.resize(pos);
        int ret = check_object_access(parent.c_str(), X_OK, NULL);
        if (ret != 0) {
            return ret;
        }
        pos = parent.rfind('/');
    }
    return 0;
}

int BosfsUtil::check_parent_object_access(const char *path, int mask) {
    int ret = 0;
    if (mask & X_OK) {
        ret = check_path_accessible(path);
        if (ret != 0) {
            return ret;
        }
    }
    mask = (mask & ~X_OK);
    if (mask) {
        std::string parent(path);
        size_t pos = parent.rfind('/');
        if (pos > 0) {
            parent.resize(pos);
        } else {
            parent.resize(1);
        }
        ret = check_object_access(parent.c_str(), mask, NULL);
    }
    return ret;
}

int BosfsUtil::check_object_owner(const char *path, struct stat *pstbuf)
{
    struct fuse_context *pctx = NULL;
    if (NULL == (pctx = this->fuse_get_context())) {
        return -EIO;
    }
    int ret = 0;
    struct stat st;
    struct stat *pst = (pstbuf ? pstbuf : &st);
    if (0 != (ret = get_object_attribute(path, pst))) {
        return ret;
    }

    if (0 == pctx->uid) {
        return 0;
    }

    if (options().is_bosfs_uid && options().bosfs_uid == pctx->uid) {
        return 0;
    }

    if (pctx->uid == pst->st_uid) {
        return 0;
    }
    return -EPERM;
}

DataCacheEntity * BosfsUtil::get_local_entity(const char *path, bool is_load)
{
    struct stat st;
    ObjectMetaData meta;
    if (0 != get_object_attribute(path, &st, &meta)) {
        return NULL;
    }

    time_t mtime = (!S_ISREG(st.st_mode) || S_ISLNK(st.st_mode)) ? -1 : st.st_mtime;
    bool force_tmpfile = S_ISREG(st.st_mode) ? false : true;

    DataCacheEntity *ent = NULL;
    if (NULL == (ent = _data_cache->open_cache(path, &meta,
                    static_cast<ssize_t>(st.st_size), mtime, force_tmpfile, true))) {

        BOSFS_ERR("could not open file, errno = %d", errno);
        return NULL;
    }

    if (is_load && !ent->open_and_load_all(&meta)) {
        BOSFS_ERR("could not load file, errno = %d", errno);
        _data_cache->close_cache(ent);
        return NULL;
    }

    return ent;
}


// Api relating to BOS client
int BosfsUtil::create_bos_client(std::string &errmsg) {
    if (options().ak.empty() || options().sk.empty()) {
        BOSFS_ERR("initiate bos client error: %s", stringfy_ret_code(BOSFS_AK_SK_INVALID));
        return_with_error_msg(errmsg, "initiate bos client error: %s", stringfy_ret_code(BOSFS_AK_SK_INVALID));
        return BOSFS_AK_SK_INVALID;
    }
    ClientOptions option;
    option.user_agent = std::string("bosfs-" VERSION "/") + sdk_package_string();
    if (options().endpoint.empty()) {
        option.endpoint = DEFAULT_ENDPOINT;
    } else {
        option.endpoint = options().endpoint;
    }
    if (options().bos_client_timeout <= 0) {
        BOSFS_ERR("initiate bos client error: %s", stringfy_ret_code(BOSFS_TIMEOUT_INVALID));
        return_with_error_msg(errmsg, "initiate bos client error: %s", stringfy_ret_code(BOSFS_TIMEOUT_INVALID));
        return BOSFS_TIMEOUT_INVALID;
    }
    option.timeout = options().bos_client_timeout;
    option.multi_part_size = options().multipart_size;
    option.max_parallel = options().multipart_parallel;
    MutexGuard lock(&_client_mutex);
    _bos_client.reset(new Client(Credential(options().ak, options().sk, options().sts_token), option));
    return BOSFS_OK;
}

int BosfsUtil::exist_bucket(std::string &errmsg)
{
    BOSFS_INFO("check the bucket exist: %s", options().bucket.c_str());

    GetBucketLocationRequest request(options().bucket);
    GetBucketLocationResponse response;
    int ret = bos_client()->get_bucket_location(request, &response);

    if (0 != ret || response.is_fail()) {
        BOSFS_ERR("check bucket exist (%s) failed: %s, bos client errno: %d",
                options().bucket.c_str(), response.error().message().c_str(), ret);
        return_with_error_msg(errmsg, "check bucket exist (%s) failed: %s, bos client errno: %d",
                options().bucket.c_str(), response.error().message().c_str(), ret);
        return BOSFS_BOS_CLIENT_REQUEST_ERROR;
    }
    if (options().endpoint.empty()) {
        mutable_options().endpoint = response.location() + ".bcebos.com";
        return create_bos_client(errmsg);
    }
    return BOSFS_OK;
}

int BosfsUtil::create_bucket(std::string &errmsg)
{
    BOSFS_INFO("create the bucket: %s", options().bucket.c_str());

    PutBucketRequest request(options().bucket);
    PutBucketResponse response;
    int ret = bos_client()->put_bucket(request, &response);

    if (0 != ret || response.is_fail()) {
        BOSFS_ERR("create bucket (%s) failed: %s, bos client errno: %d",
                options().bucket.c_str(), response.error().message().c_str(), ret);
        return_with_error_msg(errmsg, "create bucket (%s) failed: %s, bos client errno: %d",
                options().bucket.c_str(), response.error().message().c_str(), ret);
        return BOSFS_CREATE_BUCKET_FAILED;
    }
    return BOSFS_OK;
}

int BosfsUtil::check_bucket_access()
{
    BOSFS_INFO("check the bucket access: %s", options().bucket.c_str());


    GetBucketAclRequest request(options().bucket);
    GetBucketAclResponse response;
    int ret = bos_client()->get_bucket_acl(request, &response);
    if (0 != ret || response.is_fail()) {
        BOSFS_ERR("check bucket access (%s) failed: %s, bos client errno: %d",
                options().bucket.c_str(), response.error().message().c_str(), ret);
        return BOSFS_BOS_CLIENT_REQUEST_ERROR;
    }

    bool can_read = false;
    bool can_write = false;
    std::string acl_id = response.owner().id;
    std::vector<Grant> acls = response.access_control_list();
    size_t acl_len = acls.size();
    for (size_t i = 0; i < acl_len; ++i) {
        std::vector<Grantee> &grantee = acls[i].grantee;
        std::vector<std::string> &perms = acls[i].permission;
        for (size_t j = 0; j < grantee.size(); ++j) {
            if ("*" == grantee[j].id || acl_id == grantee[j].id) {
                for (size_t k = 0; k < perms.size(); ++k) {
                    if ("READ" == perms[k]) {
                        can_read = true;
                    }
                    if ("WRITE" == perms[k]) {
                        can_write = true;
                    }
                    if ("FULL_CONTROL" == perms[k]) {
                        can_read = true;
                        can_write = true;
                    }
                }
            }
        }
    }

    if (can_read && can_write) {
        return BOSFS_OK;
    }
    return BOSFS_BUCKET_ACCESS_DENIED;
}

int BosfsUtil::head_object(const std::string &object, ObjectMetaData *meta, bool *is_dir_obj,
        bool *is_prefix) {
    BOSFS_INFO("head object request: %s/%s", options().bucket.c_str(), object.c_str());
    BceRequestContext ctx[2];
    HeadObjectRequest req(options().bucket, object);
    HeadObjectResponse res;
    HeadObjectRequest req1(options().bucket, object + "/");
    HeadObjectResponse res1;
    ctx[0].request = &req;
    ctx[0].response = &res;
    ctx[1].request = &req1;
    ctx[1].response = &res1;
    int ret = bos_client()->send_request(2, ctx);
    if (ret != 0) {
        return ret;
    }
    *is_dir_obj = false;
    *is_prefix = false;
    if (!res.is_fail()) {
        // regular file object
        if (meta != NULL) {
            meta->move_from(res.meta());
        }
        return BOSFS_OK;
    } else if (res.status_code() != 404) {
        BOSFS_WARN("head object(%s) failed, bos service error: %s", object.c_str(),
                res.error().message().c_str());
        return BOSFS_BOS_SERVICE_ERROR;
    }
    if (!res1.is_fail()) {
        // directory object
        *is_dir_obj = true;
        if (meta != NULL) {
            meta->move_from(res1.meta());
        } 
        return BOSFS_OK;
    } else if (res1.status_code() != 404) {
        BOSFS_WARN("head object(%s) failed, bos service error: %s", req1.object_name().c_str(),
                res1.error().message().c_str());
        return BOSFS_BOS_SERVICE_ERROR;
    }
    std::vector<std::string> subitems;
    if (list_subitems(object + "/", 2, &subitems) != 0) {
        return BOSFS_BOS_SERVICE_ERROR;
    }
    if (subitems.empty()) {
        return BOSFS_OBJECT_NOT_EXIST;
    }
    *is_prefix = true;
    return BOSFS_OK;
}

int BosfsUtil::multiple_head_object(std::vector<std::string> &objects,
        std::vector<struct stat *> &stats) {
    std::vector<BceRequestContext> ctx(objects.size());
    for (size_t i = 0; i < objects.size(); ++i) {
        ctx[i].request = new HeadObjectRequest(options().bucket, objects[i]);
        ctx[i].response = new HeadObjectResponse();
        ctx[i].is_own = true;
    }
    int ret = bos_client()->send_request(ctx.size(), &ctx.front(), 100);
    if (ret != 0) {
        return ret;
    }
    for (size_t i = 0; i < objects.size(); ++i) {
        HeadObjectResponse *res = (HeadObjectResponse *) ctx[i].response;
        init_default_stat(stats[i]);
        if (res->is_fail()) {
            if (!SysUtil::is_dir_path(objects[i])) {
                  stats[i]->st_mode &= ~S_IFDIR;
                  stats[i]->st_mode |= S_IFREG;
            }
            if (res->status_code() != 404) {
                LOG(WARN) << "get object " << objects[i] << "'s meta failed: "
                    << res->error().message() << ", return empty stat";
            }
        } else {
            bool is_dir_obj = false;
            std::string path = "/";
            if (*objects[i].rbegin() == '/') {
                path += objects[i].substr(0, objects[i].length() - 1);
                is_dir_obj = true;
            } else {
                path += objects[i];
            }
            FilePtr file(new File(this, path));
            file->meta().move_from(res->meta());
            file->set_is_dir_obj(is_dir_obj);
            file->stat(stats[i]);
            _file_manager->set(path, file);
        }
    }
    return 0;
}

int BosfsUtil::list_objects(const std::string &prefix, int max_keys, std::string &marker,
        const char *delimiter, std::vector<std::string> *items,
        std::vector<std::string> *common_prefix) {
    ListObjectsRequest request(options().bucket);
    if (max_keys >= 0 && max_keys < 1000) {
        request.set_max_keys(max_keys);
    }
    if (prefix == "" || SysUtil::is_dir_path(prefix)) {
        request.set_prefix(prefix);
    } else {
        request.set_prefix(prefix + "/");
    }
    if (delimiter != NULL) {
        request.set_delimiter(delimiter);
    }

    bool has_next = true;
    int n = 0;
    if (common_prefix == NULL) {
        common_prefix = items;
    }
    while (has_next && (max_keys <= 0 || n < max_keys)) {
        request.set_marker(marker);
        if (max_keys - n < 1000) {
            request.set_max_keys(max_keys - n);
        }
        ListObjectsResponse response;
        bos_client()->list_objects(request, &response);
        if (response.is_fail()) {
            LOG(ERROR) << "list objects [" << prefix << "] failed: (" << response.status_code()
                << ")" << response.error().message();
            return -1;
        }
        const std::vector<std::string> dirs = response.common_prefixes();
        for (size_t i = 0; i < dirs.size(); ++i) {
            common_prefix->push_back(dirs[i]);
        }
        n += dirs.size();

        const std::vector<ObjectSummary> &objects = response.contents();
        if (objects.size() > 0) {
            if (objects[0].key != prefix) {
                items->push_back(objects[0].key);
                n += objects.size();
            } else {
                n += objects.size() - 1;
            }
            for (size_t i = 1; i < objects.size(); ++i) {
                items->push_back(objects[i].key);
            }
        }
        marker = response.next_marker();
        has_next = response.is_truncated();
    }
    if (!has_next) {
        marker.clear();
    }
    return 0;
}

void BosfsUtil::create_meta(const std::string &object_name, mode_t mode, uid_t uid, gid_t gid,
        ObjectMetaData *meta) {
    if (S_ISLNK(mode)) {
        meta->set_content_type("application/octet-stream");
    } else if (S_ISDIR(mode)) {
        meta->set_content_type("application/x-directory");
    } else {
        meta->set_content_type(SysUtil::get_mimetype(object_name));
    }
    meta->set_user_meta("bosfs-uid", uid);
    meta->set_user_meta("bosfs-gid", gid);
    meta->set_user_meta("bosfs-mode", mode);
    meta->set_user_meta("bosfs-mtime", TimeUtil::now());
}

int BosfsUtil::create_object(const char *path, mode_t mode, uid_t uid, gid_t gid) {
    static const std::string emptystr;
    return create_object(path, mode, uid, gid, emptystr);
}

int BosfsUtil::create_object(const char *path, mode_t mode, uid_t uid, gid_t gid,
        const std::string &data) {
    BOSFS_INFO("[path=%s][mode=%04o]", path, mode);

    std::string object_name = path + 1;
    ObjectMetaData meta;
    create_meta(object_name, mode, uid, gid, &meta);
    
    if (S_ISDIR(mode)) {
        object_name += "/";
    }
    PutObjectRequest request = PutObjectRequest(options().bucket, object_name);
    if (!data.empty()) {
        request.set_data(data);
    }
    PutObjectResponse response;
    request.set_meta(&meta);
    int ret = bos_client()->put_object(request, &response);

    if (0 != ret || response.is_fail()) {
        BOSFS_ERR("create object(%s) failed: %s, bos client errno: %d",
                path, response.error().message().c_str(), ret);
        return BOSFS_BOS_CLIENT_REQUEST_ERROR;
    }
    return BOSFS_OK;
}

int BosfsUtil::delete_object(const std::string &object, std::string *version)
{
    BOSFS_INFO("delete object request: %s", object.c_str());

    DeleteObjectRequest request(options().bucket, object);
    DeleteObjectResponse response;
    int ret = bos_client()->delete_object(request, &response);
    if (0 != ret) {
        BOSFS_ERR("delete object(%s) failed, bos client errno: %d", object.c_str(), ret);
        return BOSFS_BOS_CLIENT_REQUEST_ERROR;
    }
    if (response.is_fail()) {
        if (response.status_code() != 404) {
            BOSFS_WARN("delete object(%s) failed, bos service error: %s", object.c_str(),
                    response.error().message().c_str());
            return BOSFS_BOS_SERVICE_ERROR;
        }
        return BOSFS_OBJECT_NOT_EXIST;
    }
    if (NULL != version) {
        *version = response.version();
    }
    return BOSFS_OK;
}

int BosfsUtil::change_object_meta(const std::string &object, ObjectMetaData &meta) {
    int ret = bos_client()->copy_object(options().bucket, object, options().bucket, object, "", &meta);
    if (ret == RET_KEY_NOT_EXIST) {
        //retry for 5 times, in case that object have not been flushed to bos
        for (int i = 0; i < 5; ++i) {
            ret = bos_client()->copy_object(options().bucket, object, options().bucket, object, "", &meta);
            if (ret == 0) {
                return 0; 
            }
            ::sleep(1);
        }
        return -ENOENT;
    } else if (ret != 0) {
        return -EIO;
    }
    return 0;
}

int BosfsUtil::rename_file(const std::string &src, const std::string &dst, int64_t size_hint) {
    BOSFS_INFO("copy object request from: %s to: %s", src.c_str(), dst.c_str());
    int ret = 0;
    if (size_hint < 0 || size_hint >= options().multipart_threshold) {
        ret = bos_client()->parallel_copy(options().bucket, src, options().bucket, dst, options().storage_class);
    } else {
        ret = bos_client()->copy_object(options().bucket, src, options().bucket, dst, options().storage_class);
    }
    if (ret != 0) {
        _file_manager->del("/" + src.substr(0, src.size()-1));
        _file_manager->del("/" + dst.substr(0, dst.size()-1));
        return ret;
    }
    delete_object(src);

    _file_manager->del("/" + src.substr(0, src.size()-1));
    _file_manager->del("/" + dst.substr(0, dst.size()-1));
    return 0;
}

int BosfsUtil::rename_directory(const std::string &src, const std::string &dst) {
    std::string prefix;
    std::string dst_prefix;
    if (*src.rbegin() == '/') {
        prefix = src;
    } else {
        prefix = src + '/';
    }
    if (*dst.rbegin() == '/') {
        dst_prefix = dst;
    } else {
        dst_prefix = dst + '/';
    }
    std::string marker;
    std::vector<std::string> items;
    int ret = list_objects(prefix, -1, marker, NULL, &items);
    if (ret != 0) {
        return ret;
    }
    std::vector<std::string> dst_items;
    for (size_t i = 0; i < items.size(); ++i) {
        std::string dst_object = dst_prefix + items[i].substr(prefix.length(),
                items[i].length() - prefix.length());
        ret = bos_client()->copy_object(options().bucket, items[i], options().bucket, dst_object, options().storage_class);
        if (ret != 0 && ret != RET_KEY_NOT_EXIST) {
            break;
        }
        dst_items.push_back(dst_object);
        _file_manager->del("/" + dst_object);
    }
    if (dst_items.size() != items.size()) {
        ret = RET_SERVICE_ERROR;
    } else {
        ret = rename_file(prefix, dst_prefix, 0);
    }
    if (ret != 0 && ret != RET_KEY_NOT_EXIST) {
        for (size_t i = 0; i < dst_items.size(); ++i) {
            delete_object(dst_items[i]);
            _file_manager->del("/" + dst_items[i]);
        }
        return BOSFS_BOS_SERVICE_ERROR;
    }
    for (size_t i = 0; i < items.size(); ++i) {
        delete_object(items[i]);
        _file_manager->del("/" + items[i]);
    }
    return 0;
}

END_FS_NAMESPACE
