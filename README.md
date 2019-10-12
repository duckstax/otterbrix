[![Build Status](https://travis-ci.org/blizgard/RocketJoe.svg?branch=master)](https://travis-ci.org/blizgard/RocketJoe)

Applications server 

This is an attempt to provide very easy to use Lua Application Server working over HTTP and WebSockets  protocol . 
RocketJoe is an application server for micro-services architecture.

### Under heavy development. Come back later

* boost >=  1.70
* cmake >=  3.14
* lua   == 5.3

## Setup Developers Environments 

### for mac os x 

```
brew install mongo-cxx-driver cmake lua  boost  ccache doxygen gperftools conan

git clone git@github.com:smart-cloud/rocketjoe.git rocketjoe

cd rocketjoe

git submodule init

git submodule update --recursive

mkdir build

cp shared/rocketjoe/config.yaml build

cp lua/web.lua build

cd build

cmake ..

make rocketjoe

./rocketjoe 

```

## for debian base

```bash

apt install git ccache g++ cmake python3 python3-dev python3-pip lua5.3 liblua5.3-dev 

pip3 install conan --upgrade 

git clone https://github.com/smart-cloud/RocketJoe.git rocketjoe

cd rocketjoe

git submodule init

git submodule update --recursive

cd rocketjoe

cmake ..

make rocketjoe

./rocketjoe 
 
```
