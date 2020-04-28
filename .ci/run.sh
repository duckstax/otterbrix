#!/bin/bash

set -e
set -x

if [[ "$(uname -s)" == 'Darwin' ]]; then
    if which pyenv > /dev/null; then
        eval "$(pyenv init -)"
    fi
    pyenv activate conan
fi

conan remote add bincrafters https://api.bintray.com/conan/bincrafters/public-conan
conan remote add jinncrafters https://api.bintray.com/conan/jinncrafters/conan
#- mkdir build && cd build
#conan install . --build=missing #-s build_type=Release --build=missing #--install-folder=build
cmake . -GNinja -DCMAKE_BUILD_TYPE=Release -DDEV_MODE=ON
cmake --build . --parallel 2
jupyter kernelspec install --user kernels/ipython
pytest core/rocketjoe/python_sandbox/test_python_kernel.py