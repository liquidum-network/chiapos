set -e
vcpkg_dir=~/storage/vcpkg
sudo apt update
sudo apt install -y --upgrade \
  build-essential tar zip unzip pkg-config cmake g++ \
  libpython3.9-dev libcurl4-openssl-dev clang-tidy libssl-dev \
  libpthread-stubs0-dev

git clone --depth=1 https://github.com/microsoft/vcpkg ${vcpkg_dir} || (cd ${vcpkg_dir} && git pull)
[ -f ${vcpkg_dir}/vcpkg ] || ${vcpkg_dir}/bootstrap-vcpkg.sh
${vcpkg_dir}/vcpkg update
${vcpkg_dir}/vcpkg upgrade --no-dry-run
${vcpkg_dir}/vcpkg install nlohmann-json spdlog

aws_build_dir=~/storage/chiapos-builds/superbuild
build_dir=~/storage/chiapos-builds/build

export CC=gcc-11
export CXX=g++-11
cmake -Hsuperbuild -B $aws_build_dir "-DCMAKE_INSTALL_PREFIX=ext/install"
cmake --build $aws_build_dir -- -j4

cmake_flags="-DCMAKE_INSTALL_PREFIX=ext/install -DBUILD_DEPS:BOOL=ON -DCMAKE_TOOLCHAIN_FILE=${vcpkg_dir}/scripts/buildsystems/vcpkg.cmake -DCMAKE_BUILD_TYPE=RELEASE"
cmake -B $build_dir -H. ${cmake_flags}
cmake --build $build_dir -- -j6 $@

target=chiapos.cpython-39-x86_64-linux-gnu.so
if [ -f ~/storage/chiapos-builds/build/$target ]; then
  cp ~/storage/chiapos-builds/build/$target build/${target}.tmp
  mv build/${target}.tmp build/${target}
  echo copied python library into build/${target}
fi

target=verifier_py.cpython-39-x86_64-linux-gnu.so
if [ -f ~/storage/chiapos-builds/build/$target ]; then
  cp ~/storage/chiapos-builds/build/$target build/${target}.tmp
  mv build/${target}.tmp build/${target}
  echo copied python library into build/${target}
fi
