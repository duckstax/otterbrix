
Applications server 

This is an attempt to provide very easy to use Lua Application Server working over HTTP and WebSockets  protocol . 
RocketJoe is an application server for micro-services architecture.

### Under heavy development. Come back later

* boost > 1.68
* cmake > 2.8
* lua   > 5.3

## Setup Developers Environments

### for mac os x 

```
brew install mongo-cxx-driver cmake lua  boost  ccache doxygen gperftools

git clone git@github.com:smart-cloud/rocketjoe.git rocketjoe

cd rocketjoe

sudo docker-compose up # monodb up

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

### use conan  

```bash

apt install git ccache g++ cmake python3 python3-dev python3-pip lua5.3 liblua5.3-dev 

pip3 install conan --upgrade 

conan remote add bisect https://api.bintray.com/conan/bisect/bisect

conan remote add bincrafters https://api.bintray.com/conan/bincrafters/public-conan

git clone https://github.com/smart-cloud/RocketJoe.git rocketjoe

git submodule init

git submodule update --recursive

cd rocketjoe

cmake ..

make rocketjoe

./rocketjoe 
 
```

### non use conan


```
    apt-get install -y \ 
            g++\
            make\
            cmake\ 
            wget\ 
            autoconf\
            automake\
            autotools-dev\
            bsdmainutils\
            build-essential\
            doxygen\
            git\
            ccache\
            libreadline-dev\
            libtool\
            ncurses-dev\
            pbzip2\
            pkg-config\
            python3\
            python3-dev\
            python3-pip 
            lua5.3\
            liblua5.3-dev 
        
    wget -c https://www.openssl.org/source/openssl-1.0.2p.tar.gz
    
    tar zxfv openssl-1.0.2p.tar.gz
    
    cd ~/openssl-1.0.2p
    
    ./config shared
    
    make
     
    
    
    wget http://downloads.sourceforge.net/project/boost/boost/1.68.0/boost_1_68_0.tar.gz
    
    tar zxfv boost_1_68_0.tar.gz
    
    cd ~/boost_1_68_0
    
    ./bootstrap.sh --prefix=/usr/local/boost168 --with-libraries=program_options,filesystem,regex,timer,locale,serialization,system,thread,test
    
    ./b2
    
    ./b2 instal 
    
    
       
    
    mkdir libmongoc
 
    wget -c https://github.com/mongodb/mongo-c-driver/archive/1.12.0.tar.gz
     
    tar xzf 1.12.0.tar.gz 
    
    cd mongo-c-driver 
     
    cmake -DENABLE_TESTS:BOOL=OFF -DENABLE_STATIC:BOOL=OFF -DENABLE_EXAMPLES:BOOL=OFF -DENABLE_EXTRA_ALIGNMENT:BOOL=OFF -DCMAKE_INSTALL_PREFIX:PATH=/home/kotbegemot/CLionProjects/libmongoc -DOPENSSL_INCLUDE_DIR=/home/kotbegemot/CLionProjects/openssl-1.0.2p/include -DOPENSSL_SSL_LIBRARY=/home/kotbegemot/CLionProjects/openssl-1.0.2p/libssl.so -DOPENSSL_CRYPTO_LIBRARY=/home/kotbegemot/CLionProjects/openssl-1.0.2p/libcrypto.so -DCMAKE_BUILD_TYPE=Release
    
    make 
    
    make install
    
    
    
    
    mkdir libmongocxx
    
    wget -c https://github.com/mongodb/mongo-cxx-driver/archive/r3.3.1.tar.gz

    tar xzf r3.3.1.tar.gz
        
    cd mongo-cxx-driver
       
    cmake -DBUILD_SHARED_LIBS:BOOL=ON -DENABLE_TESTS:BOOL=OFF -DENABLE_EXAMPLES:BOOL=OFF -DBSONCXX_POLY_USE_BOOST:BOOL=OFF -DBSONCXX_POLY_USE_MNMLSTC:BOOL=ON -DBSONCXX_POLY_USE_STD:BOOL=OFF -Dlibmongoc-1.0_DIR:PATH=/home/kotbegemot/CLionProjects/libmongoc/lib/cmake/libmongoc-1.0/ -Dlibbson-1.0_DIR:PATH=/home/kotbegemot/CLionProjects/libmongoc/lib/cmake/libbson-1.0/  -DCMAKE_INSTALL_PREFIX:PATH=/home/kotbegemot/CLionProjects/libmongocxx  -DCMAKE_BUILD_TYPE=Release
    
    make 
    
    make install
    
    
    сcmake -DBoost_NO_BOOST_CMAKE=TRUE -DBoost_NO_SYSTEM_PATHS=TRUE -DBOOST_ROOT=/home/kotbegemot/CLionProjects/boost_1_67_0/ -DBoost_LIBRARY_DIRS=/home/kotbegemot/CLionProjects/boost_1_67_0/stage/lib/ -Dlibmongocxx_DIR=/home/kotbegemot/CLionProjects/libmongocxx/lib/cmake/libmongocxx-3.3.1 -Dlibbsoncxx_DIR=/home/kotbegemot/CLionProjects/libmongocxx/lib/cmake/libbsoncxx-3.3.1 ..
    
    

    cmake -DCMAKE_INSTALL_PREFIX=$TARGET \
                  
```

### build for docker

```

``` 