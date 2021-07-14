CMAKE_MINIMUM_REQUIRED(VERSION 3.14 FATAL_ERROR)

project(third_part)


find_package(OpenSSL REQUIRED)


# NOTE: I've been removing the call to sort_links(EXPORTS) from sdks.cmake,
# otherwise this doesn't build anymore. Nothing changed though, so I'm not sure
# why it isn't building (tested with 1.9.20 and 1.9.22).
include(ExternalProject)
ExternalProject_Add(aws-sdk-cpp
    GIT_REPOSITORY    https://github.com/aws/aws-sdk-cpp.git
    GIT_TAG           1.9.38
    GIT_SHALLOW       TRUE
    LIST_SEPARATOR     "|"
    #EXCLUDE_FROM_ALL  TRUE
    #BUILD_ALWAYS      TRUE
    UPDATE_COMMAND    ""
    TEST_COMMAND      ""
    CMAKE_ARGS        -DBUILD_SHARED_LIBS=OFF
                      -DFORCE_SHARED_CRT=OFF
                      -DBUILD_ONLY=core|s3|s3-crt # logs?
                      -DCMAKE_BUILD_TYPE=Release
                      -DBUILD_DEPS=ON
                      -DCPP_STANDARD=20
                      -DENABLE_TESTING=OFF
                      -DENABLE_VIRTUAL_OPERATIONS=OFF
                      -DENABLE_UNITY_BUILD=ON
                      -DENABLE_RTTI=ON
                      -DCUSTOM_MEMORY_MANAGEMENT=OFF
                      -DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}
                      -DSIMPLE_INSTALL=ON
                      -DOPENSSL_USE_STATIC_LIBS=ON
                      -DCMAKE_FIND_LIBRARY_SUFFIXES=".a"
                      -DCMAKE_BUILD_SHARED=OFF
                      -DCMAKE_POLICY_DEFAULT_CMP0077=NEW
                      -DCMAKE_CXX_FLAGS="-Wno-uninitialized"
)
