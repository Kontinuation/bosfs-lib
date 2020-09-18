/**
 * bosfs - A fuse-based file system implemented on Baidu Object Storage(BOS)
 *
 * Copyright (c) 2016 Baidu.com, Inc. All rights reserved.
 *
 * @file    util.cpp
 * @author  Song Shuangyang(songshuangyang@baidu.com)
 * @date    2016.8
 **/
#include "util.h"
#include <stdlib.h>

BEGIN_FS_NAMESPACE

// Facility of return code definition
const char * stringfy_ret_code(int code)
{
    switch (code) {
        case BOSFS_OK:
            return "OK";
        case BOSFS_BOS_CLIENT_UNINITIALIZED:
            return "BOS Client is not initialized";
        case BOSFS_BOS_CLIENT_REQUEST_ERROR:
            return "BOS Client sending request occurs error";
        case BOSFS_CREATE_BUCKET_FAILED:
            return "Create bucket failed";
        case BOSFS_BUCKET_NOT_EXISTS:
            return "Request bucket is not exist";
        case BOSFS_BUCKET_ACCESS_DENIED:
            return "No enough level to access the bucket";
        case BOSFS_AK_SK_INVALID:
            return "invalid ak or sk parameters";
        case BOSFS_HOST_INVALID:
            return "invalid host/endpoint/protocol parameters";
        case BOSFS_TIMEOUT_INVALID:
            return "invalid bos client timeout parameter";
        case BOSFS_MEMORY_ERROR:
            return "memory occurs error";
        case BOSFS_ADD_META_CACHE_FAIL:
            return "failed to add meta cache";
        case BOSFS_CONVERT_HEADER_TO_META_CACHE_FAIL:
            return "failed to convert header to meta cache";
        case BOSFS_LIST_OBJECTS_FAIL:
            return "failed to list objects of the bucket";
        case BOSFS_BUCKET_NOT_EMPTY:
            return "the bucket is not empty";
        case BOSFS_OBJECT_KEY_INVALID:
            return "object key is invalid";
        case BOSFS_NOT_ALLOWED_OPERATION:
            return "the operation is not allowed";

        default:
            return "OK";
    };
    return "";
}

END_FS_NAMESPACE

