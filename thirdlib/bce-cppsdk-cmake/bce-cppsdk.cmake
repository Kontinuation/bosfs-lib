# Integrates pybind11 at configure time.  Based on the instructions at
# https://github.com/google/pybind11/tree/master/pybind11#incorporating-into-an-existing-cmake-project

# Set up the external pybind11 project, downloading the latest from Github
# master if requested.
configure_file(
  ${CMAKE_CURRENT_LIST_DIR}/CMakeLists.txt.in
  ${CMAKE_BINARY_DIR}/bce-cppsdk-download/CMakeLists.txt
)

# configure_file(third_party/CMakeLists.txt.in bce-cppsdk-download/CMakeLists.txt)
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/bce-cppsdk-download )
if(result)
  message(FATAL_ERROR "CMake step for bce-cppsdk failed: ${result}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} --build .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/bce-cppsdk-download )
if(result)
  message(FATAL_ERROR "Build step for bce-cppsdk failed: ${result}")
endif()

# an extra cmake setup step
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" ${CMAKE_CURRENT_BINARY_DIR}/bce-cppsdk-src
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/bce-cppsdk-build )
if(result)
  message(FATAL_ERROR "CMake step for bce-cppsdk-src failed: ${result}")
endif()

# Add bce-cppsdk directly to our build
add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/bce-cppsdk-src
                 ${CMAKE_CURRENT_BINARY_DIR}/bce-cppsdk-build
                 EXCLUDE_FROM_ALL)
