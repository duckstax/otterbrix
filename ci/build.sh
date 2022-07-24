#!/usr/bin/env sh

source ./activate.sh

cmake .. -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=$EMSCRIPTEN/cmake/Modules/Platform/Emscripten.cmake -DDEV_MODE=OFF -DWASM_BUILD=ON
cmake --build .

cmake .. -G Ninja -DCMAKE_BUILD_TYPE=Release -DDEV_MODE=OFF -DWASM_BUILD=ON
cmake --build .

ctest -V
pytest
