#!/bin/bash

set -e
set -x

if [[ "$(uname -s)" == 'Darwin' ]]; then
    brew update
    brew outdated pyenv || brew upgrade pyenv
    brew install pyenv-virtualenv
    brew install cmake || true

    if which pyenv > /dev/null; then
        eval "$(pyenv init -)"
    fi

    pyenv install 3.7.0
    pyenv virtualenv 3.7.0 conan
    pyenv rehash
    pyenv activate conan
fi

sudo apt install ninja-build
pip install conan conan_package_tools bincrafters_package_tools  --upgrade
pip3 install -r requirements.txt

conan user