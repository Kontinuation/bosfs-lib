# BOS FS: Mount BOS Buckets to Local Directory using FUSE

This is a modified version of the publicly available bosfs source code.

1. Build using CMake
2. Contains a library interface (`include/bosfs_lib/bosfs_lib.h`)
3. Switched to libfuse3

You can refer to `src/main.cpp` and `test/test_bosfs_lib.cpp` to have a understanding of how to
integrate bosfs_lib into your known application.
