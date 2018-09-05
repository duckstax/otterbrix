
Applications server 

This is an attempt to provide very easy to use Lua Application Server working over HTTP and WebSockets  protocol . 
RocketJoe is an application server for micro-services architecture.

### Under heavy development. Come back later

##RoadMap  
1. server ws + http
2. json rpc 2.0
3. DataBase MongoDb
4. client http/ws
5. cron-like
6. p2p


boost > 1.67
cmake > 2.8
lua   > 5.3



    apt-get install -y \
        autoconf \
        automake \
        autotools-dev \
        bsdmainutils \
        build-essential \
        cmake \
        doxygen \
        git \
        ccache\
        libboost-all-dev \
        libreadline-dev \
        libssl-dev \
        libtool \
        ncurses-dev \
        pbzip2 \
        pkg-config \
        lua5.3\
        liblua5.3-dev \
        python3 \
        python3-dev \
        python3-pip \
    
    pip3 install gcovr

    cmake -DCMAKE_INSTALL_PREFIX=$TARGET \
        -DBoost_NO_BOOST_CMAKE=TRUE \
        -DBoost_NO_SYSTEM_PATHS=TRUE \
        -DBOOST_ROOT:PATHNAME=/home/kotbegemot/CLionProjects/boost_1_67_0/boost/ \
        -DBoost_LIBRARY_DIRS:FILEPATH=/home/kotbegemot/CLionProjects/boost_1_67_0/stage/lib/lib
        
        -DENABLE_PYTHON3=ON -DPYTHON_EXECUTABLE:FILEPATH=/usr/bin/python3  -DBUILD_LUA=False
        
        
                 
        docker 
        
        # installing mongo drivers
        RUN \
            echo "Installing mongo-c-driver" && \
            apt-get -qq update && \
            apt-get install -y \
                pkg-config \
                libssl-dev \
                libsasl2-dev \
                wget \
            && \
            wget https://github.com/mongodb/mongo-c-driver/releases/download/1.9.5/mongo-c-driver-1.9.5.tar.gz && \
            tar xzf mongo-c-driver-1.9.5.tar.gz && \
            cd mongo-c-driver-1.9.5 && \
            ./configure --disable-automatic-init-and-cleanup --enable-static && \
            make && \
            make install && \
            cd .. && \
            rm -rf mongo-c-driver-1.9.5 && \
            echo "Installing mongo-cxx-driver" && \
            git clone https://github.com/mongodb/mongo-cxx-driver.git --branch releases/v3.2 --depth 1 && \
            cd mongo-cxx-driver/build && \
            cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local .. && \
            make EP_mnmlstc_core && \
            make && \
            make install && \
            cd ../.. && \
            rm -rf mongo-cxx-driver
        # end