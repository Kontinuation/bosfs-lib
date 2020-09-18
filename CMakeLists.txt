cmake_minimum_required (VERSION 3.1)
project (bosfs)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")

if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_STANDARD 14)
add_definitions(-Wall -Wextra -g -D_FILE_OFFSET_BITS=64 -DFUSE_USE_VERSION=35 -DINCLUDE_FUSE3)
set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
set (CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

include(thirdlib/bce-cppsdk-cmake/bce-cppsdk.cmake)

set(MAIN_SRCS
  src/bosfs_impl.cpp
  src/bosfs_lib.cpp
  src/bosfs_util.cpp
  src/data_cache.cpp
  src/file_manager.cpp
  src/sys_util.cpp
  src/util.cpp
)

add_definitions(
	-DVERSION="1.0"
)

find_package(fuse3 REQUIRED)

add_library(bosfs_static STATIC ${MAIN_SRCS})
target_include_directories(bosfs_static PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include ${FUSE3_INCLUDE_DIR})
target_link_libraries(bosfs_static PUBLIC bossdk)

add_executable(bosfs src/main.cpp)
target_include_directories(bosfs PRIVATE include)
target_link_libraries(bosfs bosfs_static ${FUSE3_LIBRARIES})

add_executable(test_bosfs_lib test/test_bosfs_lib.cpp)
target_include_directories(test_bosfs_lib PRIVATE include)
target_link_libraries(test_bosfs_lib bosfs_static ${FUSE3_LIBRARIES})
