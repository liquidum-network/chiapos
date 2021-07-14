CMAKE_MINIMUM_REQUIRED(VERSION 3.14 FATAL_ERROR)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

IF(NOT CMAKE_BUILD_TYPE)
  SET(CMAKE_BUILD_TYPE "RELEASE")
ENDIF()

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
endif()

project(chiapos C CXX ASM)

# CMake 3.14+
include(FetchContent)

FetchContent_Declare(
  pybind11-src
  GIT_REPOSITORY https://github.com/pybind/pybind11.git
  GIT_TAG        v2.6.2
)
FetchContent_MakeAvailable(pybind11-src)

FetchContent_Declare(
  cxxopts
  GIT_REPOSITORY https://github.com/jarro2783/cxxopts.git
  GIT_TAG        v2.2.1
)
FetchContent_MakeAvailable(cxxopts)

FetchContent_Declare(
  fmt
  GIT_REPOSITORY https://github.com/fmtlib/fmt.git
  GIT_TAG        7.1.3
)
FetchContent_MakeAvailable(fmt)

FetchContent_Declare(
  concurrentqueue
  GIT_REPOSITORY https://github.com/cameron314/concurrentqueue.git
  GIT_TAG        v1.0.3
)
FetchContent_MakeAvailable(concurrentqueue)

FetchContent_Declare(
  readerwriterqueue
  GIT_REPOSITORY https://github.com/cameron314/readerwriterqueue.git
  GIT_TAG        v1.0.5
)
FetchContent_MakeAvailable(readerwriterqueue)

if(DEFINED ENV{USE_MOCK_S3})
  set(USE_MOCK_S3 $ENV{USE_MOCK_S3})
ENDIF()
if (USE_MOCK_S3)
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_MOCK_S3=1")
ENDIF()

if(DEFINED ENV{MOCK_S3_SLEEP_TIME})
  set(MOCK_S3_SLEEP_TIME $ENV{MOCK_S3_SLEEP_TIME})
ELSE()
  set(MOCK_S3_SLEEP_TIME "0")
ENDIF()

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DMOCK_S3_SLEEP_TIME=${MOCK_S3_SLEEP_TIME}")

if(DEFINED ENV{USE_AWS_CRT})
  set(USE_AWS_CRT $ENV{USE_AWS_CRT})
ELSE()
  set(USE_AWS_CRT "0")
ENDIF()

if(DEFINED ENV{ASAN})
  if($ENV{ASAN} STREQUAL "1")
    set(ASAN TRUE)
  ENDIF()
ENDIF()

if(DEFINED ENV{TSAN})
  if($ENV{TSAN} STREQUAL "1")
    set(TSAN TRUE)
  ENDIF()
ENDIF()

set(FSE_LIB ${CMAKE_CURRENT_SOURCE_DIR}/lib/FiniteStateEntropy/lib)
set(FSE_FILES
    ${FSE_LIB}/fse_compress.c
    ${FSE_LIB}/fse_decompress.c
    ${FSE_LIB}/entropy_common.c
    ${FSE_LIB}/hist.c
)

include_directories(
  ${INCLUDE_DIRECTORIES}
  ${PROJECT_SOURCE_DIR}/lib/include
  ${cxxopts_SOURCE_DIR}/include
  ${gulrak_SOURCE_DIR}/include/ghc
  ${PROJECT_SOURCE_DIR}/lib/FiniteStateEntropy/lib
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/test
  )

add_library(fse STATIC ${FSE_FILES})

set(CXX_SANITIZE_FLAGS
  "-fsanitize=address -fsanitize=undefined")

cmake_policy(SET CMP0065 NEW)
# frtti is necessary to subclass Aws classes.
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -ggdb3  -march=native -std=c++20 -static-libgcc -static-libstdc++ -frtti -fno-semantic-interposition")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -L/usr/lib/gcc/x86_64-linux-gnu/11 -l:libstdc++.a -I/usr/include/c++/11")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -ggdb3  -march=native -static-libgcc")
set (CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -Wl,--gc-sections")
set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--gc-sections")

IF (MSVC)
IF (CMAKE_BUILD_TYPE STREQUAL "RELEASE")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /O2 /Oy")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /O2 /Oy")
ELSE()
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Ob1")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /Ob1")
ENDIF()

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Zi")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /Zi")

ELSE()
IF (CMAKE_BUILD_TYPE STREQUAL "RELEASE")

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -fmerge-all-constants -fno-stack-protector -fvisibility=hidden -flto -DNDEBUG")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3 -fno-stack-protector -fvisibility=hidden -flto -DNDEBUG")
set (CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -flto")
set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -flto")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG")
ELSE()
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -DDEBUG -Wno-terminate")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0 -DDEBUG")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG")
ENDIF()

ENDIF()

IF (ASAN)
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -fsanitize=address -fsanitize=undefined")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-omit-frame-pointer -fsanitize=address -fsanitize=undefined")
set (CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fno-omit-frame-pointer -fsanitize=address -fsanitize=undefined")
ENDIF()

IF (TSAN)
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -fsanitize=thread")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-omit-frame-pointer -fsanitize=thread")
set (CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fno-omit-frame-pointer -fsanitize=thread")
ENDIF()

set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${CMAKE_LINKER_FLAGS}")

IF (APPLE)
# on macOS "uname -m" returns the architecture (x86_64 or arm64)
execute_process(
    COMMAND uname -m
    RESULT_VARIABLE result
    OUTPUT_VARIABLE OSX_NATIVE_ARCHITECTURE
    OUTPUT_STRIP_TRAILING_WHITESPACE)
ENDIF()

IF (WIN32)
set(BLAKE3_SRC
    src/b3/blake3.c
    src/b3/blake3_portable.c
    src/b3/blake3_dispatch.c
    src/b3/blake3_avx2.c
    src/b3/blake3_avx512.c
    src/b3/blake3_sse41.c
    src/b3/blake3_sse2.c
)
ELSEIF(OSX_NATIVE_ARCHITECTURE STREQUAL "arm64")
set(BLAKE3_SRC
    src/b3/blake3.c
    src/b3/blake3_portable.c
    src/b3/blake3_dispatch.c
)
ELSE()
set(BLAKE3_SRC
    src/b3/blake3.c
    src/b3/blake3_portable.c
    src/b3/blake3_dispatch.c
    src/b3/blake3_avx2_x86-64_unix.S
    src/b3/blake3_avx512_x86-64_unix.S
    src/b3/blake3_sse41_x86-64_unix.S
    src/b3/blake3_sse2_x86-64_unix.S
)
ENDIF()
add_library(blake3 STATIC ${BLAKE3_SRC})

set(CXX_SRC
  src/encoding.cpp
  src/calculate_bucket.cpp
)
set(S3_SRC
  src/s3_utils.cpp
  src/aws_globals.cpp
  src/aws_async.cpp
  src/curl_http_client.cpp
)
set(PLOTTER_SRC
  src/logging_helpers.cpp
  src/manifest.cpp
  src/multipart_upload_file.cpp
  src/aws_async.cpp
  src/s3_plotter.cpp
  src/uniformsort.cpp
  src/s3_sync_read_file.cpp
  src/s3_sync_write_file.cpp
  src/readonly_sort_manager.cpp
  src/quicksort_entries.cpp
  src/locking_concurrent_streambuf.cpp
  src/push_streambuf.cpp
  src/s3_read.cpp
  src/s3_delete.cpp
  src/phase1.cpp
  src/phase2.cpp
  src/phase3_s3.cpp
  src/phase4_s3.cpp
  src/plot_manager.cpp
  src/managed_memory.cpp
)
set(CLI_SRC
  src/plot_manager.cpp
)

pybind11_add_module(chiapos ${CMAKE_CURRENT_SOURCE_DIR}/python-bindings/chiapos.cpp src/chacha8.c
  ${CXX_SRC}
  ${S3_SRC}
  src/plot_file_cache.cpp
  src/prover_s3.cpp
  )

pybind11_add_module(verifier_py ${CMAKE_CURRENT_SOURCE_DIR}/python-bindings/verifier_py.cpp
  src/chacha8.c
  ${CXX_SRC}
  )

add_executable(ProofOfSpace
  src/cli.cpp
  src/chacha8.c
  ${CXX_SRC}
  ${S3_SRC}
  ${PLOTTER_SRC}
  ${CLI_SRC}
  src/prover_s3.cpp
  src/plot_file_cache.cpp
)
target_compile_options(ProofOfSpace PRIVATE -DUSE_AWS_CRT=${USE_AWS_CRT})

add_executable(RunTests
  tests/test-main.cpp
  tests/test.cpp
  src/chacha8.c
  ${CXX_SRC}
)

add_executable(S3FileTest
  tests/s3-file-test.cpp
  ${S3_SRC}
  ${PLOTTER_SRC}
)

add_executable(StreambufTest
  tests/push_streambuf_test.cpp
  src/push_streambuf.cpp
  src/locking_concurrent_streambuf.cpp
  src/managed_memory.cpp
)

add_executable(SortTest
  tests/sort_test.cpp
)

add_executable(S3PutTest
  tests/s3_put_test.cpp
)

find_package(Threads REQUIRED)

add_definitions(-DCURL_STATICLIB)
find_package(OpenSSL REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_path(SPDLOG_INCLUDE_DIRS spdlog/spdlog.h)

if(USE_MOCK_S3)
ELSE()
find_package(AWSSDK REQUIRED COMPONENTS core s3 s3-crt)
ENDIF()

include_directories(${AWSSDK_INCLUDE_DIRS})
include_directories(${SPDLOG_INCLUDE_DIRS})

target_link_libraries(chiapos PRIVATE
  ${AWSSDK_LIBRARIES}
  nlohmann_json
  nlohmann_json::nlohmann_json
  spdlog::spdlog
  fmt::fmt
  concurrentqueue
  blake3 fse stdc++fs Threads::Threads
)

target_link_libraries(ProofOfSpace PRIVATE
  ${AWSSDK_LIBRARIES}
  nlohmann_json
  nlohmann_json::nlohmann_json
  spdlog::spdlog
  fmt::fmt
  concurrentqueue
  readerwriterqueue
  blake3 fse stdc++fs Threads::Threads
)

target_link_libraries(RunTests PRIVATE
  fmt::fmt
  spdlog::spdlog
  concurrentqueue
  blake3 fse stdc++fs Threads::Threads
)

target_link_libraries(S3FileTest PRIVATE
  fmt::fmt
  spdlog::spdlog
  stdc++fs
  concurrentqueue
  Threads::Threads
)
target_link_libraries(StreambufTest PRIVATE
  fmt::fmt
  spdlog::spdlog
  concurrentqueue
  Threads::Threads
)
target_link_libraries(SortTest PRIVATE
  fmt::fmt
  spdlog::spdlog
)

target_link_libraries(S3PutTest PRIVATE
  ${AWSSDK_LIBRARIES}
  Threads::Threads
)

add_library(uint128 STATIC uint128_t/uint128_t.cpp)
target_include_directories(uint128 PUBLIC uint128_t)

target_compile_features(blake3 PUBLIC cxx_std_20)
target_compile_features(fse PUBLIC cxx_std_20)
target_compile_features(chiapos PUBLIC cxx_std_20)
target_compile_features(RunTests PUBLIC cxx_std_20)
target_compile_features(S3FileTest PUBLIC cxx_std_20)
target_compile_features(StreambufTest PUBLIC cxx_std_20)
target_compile_features(SortTest PUBLIC cxx_std_20)
target_compile_features(S3PutTest PUBLIC cxx_std_20)

enable_testing()
add_test(NAME RunTests COMMAND RunTests)
