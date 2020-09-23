/**
 * Copyright 2014 (c) Baidu, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>

#include <string>
#include <iostream>
#include <stdarg.h>
#include <signal.h>

#include "file_manager.h"

#include "bosfs_lib/bosfs_lib.h"
#include "bosfs_util.h"
#include "sys_util.h"

#ifndef PACKAGE_STRING
#define PACKAGE_STRING "bosfs " VERSION
#endif
#define LICENSE "Copyright (c) 2014 Baidu.com, Inc. All rights reserved."

BEGIN_FS_NAMESPACE

class FileGuard {
public:
    FileGuard() : _fp(NULL) {}
    ~FileGuard() {
        if (_fp != NULL) {
            fclose(_fp);
        }
    }

    void set(FILE *fp) {
        _fp = fp;
    }
private:
    FILE *_fp;
};
FileGuard g_log_file;

enum FuseArgsKey {
    FUSE_O_UID,
    FUSE_O_GID,
    FUSE_O_UMASK,
    FUSE_O_ALLOW_OTHER
};

struct BosfsConfItem {
    BosfsConfItem() : is_set(false) {}
    BosfsConfItem(const std::string &short_name,
            const std::string &value_prompt) : is_set(false) {
        this->short_name = short_name;
        this->value_prompt = value_prompt;
    }
    BosfsConfItem(const std::string &short_name,
            const std::string &value_prompt,
            const std::string &description) : is_set(false) {
        this->short_name = short_name;
        this->value_prompt = value_prompt;
        this->description = description;
    }
    std::string short_name;

    bool is_set;
    std::string value;
    std::string value_prompt;
    std::string description;
};

// fuse arguments that bos cares
static std::map<std::string, int> s_fuse_args;

// bos own arguments
static std::map<std::string, BosfsConfItem> s_bos_args;

// short name map
static std::map<std::string, std::string> s_old_bos_args;

// bosfs options
static BosfsOptions s_bosfs_options;

static void set_log_level(const std::string &level) {
    BOSFS_INFO("loglevel set to %s", level.c_str());
    int l = baidu::bos::cppsdk::LogUtil::string_to_level(level.c_str());
    if (l >= 0) {
        baidu::bos::cppsdk::sdk_set_log_level((bcesdk_ns::LogLevel) l);
    }
}

static void set_log_file(const std::string &file) {
    FILE *fp = fopen(file.c_str(), "a");
    if (fp != NULL) {
        g_log_file.set(fp);
        baidu::bos::cppsdk::sdk_set_log_stream(fp);
    }
}

static void init_bos_args() {
    s_fuse_args["uid"] = FUSE_O_UID;
    s_fuse_args["gid"] = FUSE_O_GID;
    s_fuse_args["umask"] = FUSE_O_UMASK;
    s_fuse_args["allow_other"] = FUSE_O_ALLOW_OTHER;

    s_bos_args["bos.fs.log.file"] = BosfsConfItem("logfile",
            "file path like xxx/xxx.log");
    s_bos_args["bos.fs.log.level"] = BosfsConfItem("loglevel",
            "level: fatal,error,warn,info,debug; case ignored");
    s_bos_args["bos.fs.endpoint"] = BosfsConfItem("endpoint",
            "url like http://bj.bcebos.com, http:// can be omitted",
            "specify server address, use https:// for SSL. if not specified, bucket's location would be used; "
            "default is bj.bcebos.com");
    s_bos_args["bos.fs.ak"] = BosfsConfItem("ak",
            "your ak");
    s_bos_args["bos.fs.sk"] = BosfsConfItem("sk",
            "your sk");
    s_bos_args["bos.fs.sts_token"] = BosfsConfItem("sts_token",
            "your sts token");
    s_bos_args["bos.fs.credentials"] = BosfsConfItem("credentials",
            "your credential file path");
    s_bos_args["bos.fs.multipart_parallel"] = BosfsConfItem("multipart_parallel", "limit the client maximum multipart parallel requests send to the server, default is 10");
    s_bos_args["bos.fs.cache.base"] = BosfsConfItem("use_cache",
            "cache directory in absolute path");
    s_bos_args["bos.fs.meta.expires"] = BosfsConfItem("meta_expires",
            "seconds", "after how many seconds the local meta will be expired, default is infinite");
    s_bos_args["bos.fs.meta.capacity"] = BosfsConfItem("meta_capacity",
            "integer number", "how many meta cache items will be keeped as a hit, default is 100000");
    s_bos_args["bos.fs.storage_class"] = BosfsConfItem("storage_class",
            "standard or standard_ia; case ignored",
            "when specified this option, any upload action will use the storage class");
    s_bos_args["bos.fs.createprefix"] = BosfsConfItem("createprefix", "",
            "create directory object if not exist when mounting");
    s_bos_args["bos.fs.tmpdir"] = BosfsConfItem("tmpdir", "an existing directory in absolute path",
            "specified where bosfs creates temporary file in, default is /tmp");
    s_bos_args["bos.sdk.multipart_size"] = BosfsConfItem("", "number small than 5GB, can use unit KB,MB",
            "an hint to part size in multiple upload, default is 10MB");
    s_bos_args["bos.sdk.multipart_threshold"] = BosfsConfItem("", "number small than 5GB, can use unit KB,MB",
            "when file is larger than this value, multiple upload will be used, default is 100MB");

    for (std::map<std::string, BosfsConfItem>::iterator it = s_bos_args.begin();
            it != s_bos_args.end(); ++it) {
        if (!it->second.short_name.empty()) {
            s_old_bos_args[it->second.short_name] = it->first;
        }
    }
}

static int parse_bos_args(BosfsOptions &bosfs_options, std::string &errmsg) {
    std::string name;
    if (s_bos_args["bos.fs.log.file"].is_set) {
        std::string logfile = s_bos_args["bos.fs.log.file"].value;
        std::string logdir = ".";
        size_t pos = logfile.rfind('/');
        if (pos != std::string::npos) {
            logdir = logfile.substr(0, pos);
        }
        if (SysUtil::check_local_dir("log", logdir, errmsg) != 0) {
            return -1;
        }
        set_log_file(logfile);
    }
    if (s_bos_args["bos.fs.log.level"].is_set) {
        set_log_level(s_bos_args["bos.fs.log.level"].value);
    }
    if (s_bos_args["bos.fs.endpoint"].is_set) {
        bosfs_options.endpoint = s_bos_args["bos.fs.endpoint"].value;
    }
    if (s_bos_args["bos.fs.ak"].is_set) {
        bosfs_options.ak = s_bos_args["bos.fs.ak"].value;
    }
    if (s_bos_args["bos.fs.sk"].is_set) {
        bosfs_options.sk = s_bos_args["bos.fs.sk"].value;
    }
    if (s_bos_args["bos.fs.sts_token"].is_set) {
        bosfs_options.sts_token = s_bos_args["bos.fs.sts_token"].value;
    }
    if (s_bos_args["bos.fs.cache.base"].is_set) {
        bosfs_options.cache_dir = s_bos_args["bos.fs.cache.base"].value;
    }
	if (s_bos_args["bos.fs.meta.expires"].is_set) {
		int secs;
		if (!StringUtil::str2int(s_bos_args["bos.fs.meta.expires"].value, &secs)) {
            return return_with_error_msg(errmsg, "invalid number: %s", s_bos_args["bos.fs.meta.expires"].value.c_str());
		}
        bosfs_options.meta_expires_s = secs;
    }
    if (s_bos_args["bos.fs.meta.capacity"].is_set) {
		int num;
		if (!StringUtil::str2int(s_bos_args["bos.fs.meta.capacity"].value, &num)) {
            return return_with_error_msg(errmsg, "invalid number: %s", s_bos_args["bos.fs.meta.capacity"].value.c_str());
		}
        bosfs_options.meta_capacity = num;
    }
    if (s_bos_args["bos.fs.createprefix"].is_set) {
       bosfs_options.create_prefix = true;
    }
    if (s_bos_args["bos.fs.storage_class"].is_set) {
        bosfs_options.storage_class = StringUtil::upper(s_bos_args["bos.fs.storage_class"].value);
    }
	if (s_bos_args["bos.fs.multipart_parallel"].is_set) {
        if (!StringUtil::str2int(s_bos_args["bos.fs.multipart_parallel"].value, &bosfs_options.multipart_parallel)) {
            return return_with_error_msg(errmsg, "%s: invalid number string %s", "bos.fs.multipart_parallel",
                s_bos_args["bos.fs.multipart_parallel"].value.c_str());
        }
    }
    if (s_bos_args["bos.fs.tmpdir"].is_set) {
        bosfs_options.tmp_dir = s_bos_args["bos.fs.tmpdir"].value;
    }
    name = "bos.sdk.multipart_size";
    if (s_bos_args[name].is_set) {
        if (!StringUtil::byteunit2int(s_bos_args[name].value, &bosfs_options.multipart_size)) {
            return return_with_error_msg(errmsg, "%s: invalid number string:%s", name.c_str(), s_bos_args[name].value.c_str());
        }
    }
    name = "bos.sdk.multipart_threshold";
    if (s_bos_args[name].is_set) {
        if (!StringUtil::byteunit2int(s_bos_args[name].value, &bosfs_options.multipart_threshold)) {
            return return_with_error_msg(errmsg, "%s: invalid number string:%s", name.c_str(), s_bos_args[name].value.c_str());
        }
    }

    return 0;
}

static void die(const char *format, ...) {
    std::string f(format);
    f += "\n";
    va_list args;
    va_start(args, format);
    vfprintf(stderr, f.c_str(), args);
    va_end (args);
    exit(EXIT_FAILURE);
}

static void show_version() {
    printf("%s\n", PACKAGE_STRING);
#ifdef BOSFS_REPO
    printf("repo: %s\n", BOSFS_REPO);
#endif
    printf("build time: %s %s\n", __DATE__, __TIME__);
    printf("%s\n", LICENSE);
    exit(0);
}

static std::string s_program_name = "bosfs";

static void show_help() {
    std::cout << "Usage: " << s_program_name << " bucket mountpoint [OPTIONS]" << std::endl
            << "OPTIONS can be fuse options, or bos options below:" << std::endl;
    std::cout << "BOS options:" << std::endl;

    for (std::map<std::string, BosfsConfItem>::iterator it = s_bos_args.begin();
            it != s_bos_args.end(); ++it) {
        std::cout << "\t";
        if (!it->second.short_name.empty()) {
            std::cout << "-o " << it->second.short_name << ", ";
        }
        std::cout << "-o " << it->first;
        if (!it->second.value_prompt.empty()) {
            std::cout << "=<" << it->second.value_prompt << ">";
        }
        std::cout << std::endl;
        if (!it->second.description.empty()) {
            std::cout << "\t\t" << it->second.description << std::endl;
        }
    }
    fprintf(stdout, "FUSE options:\n"
            "\t-f foreground mode\n"
            "\t-d debug mode\n"
            "\t-o ro\tread only mode\n"
            "\t-o fsname=<filesystem name>, shows in df command\n"
            "\t-o allow_other allow other user access mountpoint\n"
            "\t-o mount_umask when uses allow_other, use this to forbid permissions, default is 022\n");
    exit(0);
}

static std::string s_bucket_path;
static std::string s_mountpoint_path;

//命令参考：bosfs mybucket my_local_directory -o endpoint=http://bj.bcebos.com
//-o ak=xxxxxxxxxxxxxxxx -o sk=xxxxxxxxxxxxxxxxx -o logfile=xx/xx.log
static int s_noopt_arg_index = 0;
static int fuse_opt_handler(void * /*data*/, const char *arg, int key, struct fuse_args *outargs) {
    (void) outargs;
    std::string argstr(arg);
    switch (key) {
    //不带-的参数，解析bucket和挂载点
    case FUSE_OPT_KEY_NONOPT: {
        switch (s_noopt_arg_index++) {
        case 0: {//第一个参数为bucket name 或者子目录
            s_bucket_path.assign(arg);
            return 0;
         }
        case 1: {//挂载点
            s_mountpoint_path.assign(arg);
            return 1;
        }
        default:
            return 1;
        }
        break;
    }
    case FUSE_OPT_KEY_OPT: {
        std::string key;
        std::string value;
        size_t pos = argstr.find('=');
        key = StringUtil::trim(argstr.substr(0, pos));
        if (pos != std::string::npos) {//-o ak=xxxx -o sk=xxx etc..
            value = StringUtil::trim(argstr.substr(pos + 1));
        }
        if (s_old_bos_args.find(key) != s_old_bos_args.end()) {
            key = s_old_bos_args[key];
        }
        if (s_bos_args.find(key) != s_bos_args.end()) {
            s_bos_args[key].is_set = true;
            s_bos_args[key].value = value;
            return 0;
        }
        if (s_fuse_args.find(key) != s_fuse_args.end()) {
            int fuse_key = s_fuse_args[key];
            switch (fuse_key) {
            case FUSE_O_UID:
                s_bosfs_options.bosfs_uid = strtol(value.c_str(), NULL, 0);
                if (geteuid() != 0) { //root uid=0
                    die("only root user can specify uid");
                }
                s_bosfs_options.is_bosfs_uid = true;
                break;
            case FUSE_O_GID:
                s_bosfs_options.bosfs_gid = strtol(value.c_str(), NULL, 0);
                if (geteuid() != 0) {
                    die("only root user can specify gid");
                }
                s_bosfs_options.is_bosfs_gid = true;
                break;
            case FUSE_O_UMASK:
                s_bosfs_options.bosfs_mask = strtol(value.c_str(), NULL, 0);
                s_bosfs_options.bosfs_mask &= (S_IRWXU | S_IRWXG | S_IRWXO);
                s_bosfs_options.is_bosfs_umask = true;
                break;
            case FUSE_O_ALLOW_OTHER:
                s_bosfs_options.allow_other = true;
                break;
            }
            return 1;
        }
        if (key == "mount_umask") {
            s_bosfs_options.mount_umask = strtol(value.c_str(), NULL, 0);
            s_bosfs_options.mount_umask &= (S_IRWXU | S_IRWXG | S_IRWXO);
            s_bosfs_options.is_mount_umask = true;
            return 0;
        }
        if (argstr == "-h" || argstr == "--help") {
            show_help();
        }
        if (argstr == "-v" || argstr == "--version") {
            show_version();
        }
        break;
    }
    }
    return 1;
}

static int bosfs_main(int argc, char *argv[]) {
    s_program_name = argv[0];
    init_bos_args();

    // Pass custom args to fuse args handler
    struct fuse_args custom_args = FUSE_ARGS_INIT(argc, argv);
    if (0 != fuse_opt_parse(&custom_args, NULL, NULL, fuse_opt_handler)) {
        exit(EXIT_FAILURE);
    }

    std::string parse_errmsg;
    int parse_res = parse_bos_args(s_bosfs_options, parse_errmsg);
    if (parse_res != 0) {
        die("%s", parse_errmsg.c_str());
    }

    struct fuse_operations bosfs_operation;
    std::string errmsg;
    Bosfs *bosfs = new Bosfs();
    int ret = bosfs_prepare_fs_operations(s_bucket_path, s_mountpoint_path, bosfs, s_bosfs_options, bosfs_operation, errmsg);
    if (ret != 0) {
        die("preparation failed: %s", errmsg.c_str());
    }
    
    ret = fuse_main(custom_args.argc, custom_args.argv, &bosfs_operation, bosfs);
    fuse_opt_free_args(&custom_args);
    delete bosfs;
    return ret;
}

END_FS_NAMESPACE

int main(int argc, char *argv[]) {
    return bosfs_ns::bosfs_main(argc, argv);
}


