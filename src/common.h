/**
 * bosfs - A fuse-based file system implemented on Baidu Object Storage(BOS)
 *
 * Copyright (c) 2016 Baidu.com, Inc. All rights reserved.
 *
 * @file    common.h
 * @author  Song Shuangyang(songshuangyang@baidu.com)
 * @date    2016.8
 * @brief   Declare the common variables, macros and so on.
 **/
#ifndef BAIDU_BOS_BOSFS_COMMON_H
#define BAIDU_BOS_BOSFS_COMMON_H

#include <stdint.h>
#include <stdio.h>
#include <stdarg.h>
#include <sys/types.h>

#include <string>
#include <map>

#ifndef VERSION
#include "config.h"
#endif

#define BEGIN_FS_NAMESPACE \
namespace baidu { \
namespace bos { \
namespace bosfs {

#define END_FS_NAMESPACE }}}
#define bosfs_ns baidu::bos::bosfs

#define DEFAULT_ENDPOINT "bj.bcebos.com"

BEGIN_FS_NAMESPACE

// commonly used helper function
inline int return_with_error_msg(std::string &errmsg, const char *format, ...) {
    char errmsg_buf[1024];
    std::string f(format);
    va_list args;
    va_start(args, format);
    vsnprintf(errmsg_buf, sizeof(errmsg_buf), f.c_str(), args);
    va_end (args);
    errmsg.assign(errmsg_buf);
    return -1;
}

END_FS_NAMESPACE

#endif

