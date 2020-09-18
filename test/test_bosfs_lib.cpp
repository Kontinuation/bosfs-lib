#include "bosfs_lib/bosfs_lib.h"
#include <string>
#include <iostream>

using namespace baidu::bos::bosfs;

int main(int argc, char *argv[])
{
    std::string mountpoint = "./mnt";

    // NOTICE: fill in your bucket path, ak and sk here.
    std::string bucket_path;
    std::string ak;
    std::string sk;
    std::string endpoint = "http://bj.bcebos.com";

    BosfsOptions bosfs_options;
    bosfs_options.ak = ak;
    bosfs_options.sk = sk;
    bosfs_options.endpoint = endpoint;

    struct fuse_operations bosfs_operation;
    std::string errmsg;
    Bosfs *bosfs = new Bosfs();
    int ret = bosfs_prepare_fs_operations(bucket_path, mountpoint, bosfs, bosfs_options,
        bosfs_operation, errmsg);
    if (ret != 0) {
        std::cout << errmsg.c_str() << std::endl;
        delete bosfs;
        return ret;
    }

    ret = fuse_main(argc, argv, &bosfs_operation, bosfs);
    if (ret != 0) {
        std::cerr << "mount failed, ret = " << ret << std::endl;
    }
    delete bosfs;
    return ret;
}
