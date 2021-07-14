#!/bin/bash
set -e

build_base=~/storage/chiapos-builds/

if [ -z "$DEBUG" ]; then
  build_type=RELEASE
  build_dir=$build_base/build
else
  echo debug build
  build_type=DEBUG
  build_dir=$build_base/build_debug
fi


vcpkg_dir=~/storage/vcpkg
cmake_flags="-DCMAKE_ASM_COMPILER=$(which gcc-11) -DCMAKE_CXX_COMPILER=$(which g++-11) -DCMAKE_C_COMPILER=$(which gcc-11) -DCMAKE_INSTALL_PREFIX=ext/install -DBUILD_DEPS:BOOL=ON -DCMAKE_TOOLCHAIN_FILE=${vcpkg_dir}/scripts/buildsystems/vcpkg.cmake -DCMAKE_BUILD_TYPE=$build_type"

if [ "$SKIP_BUILD" == "" ]; then
  mkdir -p $build_dir

  if [ ! -f $build_dir ]; then
    cmake -B $build_dir -H. ${cmake_flags}
  fi
  cmake --build $build_dir/ -- -j6 $target_name
fi

export bin=$build_dir/$target_name
