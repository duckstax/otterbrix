FROM debian:buster-backports

RUN apt update && \
    apt upgrade -y && \
    apt install -y \
        build-essential \
        cmake \
        ninja-build \
        python3-pip && \
    apt clean && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install --no-cache-dir conan

WORKDIR /app

COPY conanfile.txt ./

RUN ln -s /usr/bin/python3 /usr/bin/python

RUN conan remote add bincrafters https://api.bintray.com/conan/bincrafters/public-conan && \
    conan remote add cyberduckninja https://api.bintray.com/conan/cyberduckninja/conan  && \
    conan install . --build missing -s build_type=Release -s compiler.libcxx=libstdc++11

COPY . ./

WORKDIR ./build

COPY tests/app/tasks.py ./

RUN cmake .. -GNinja -DCMAKE_BUILD_TYPE=Release -DDEV_MODE=ON && \
    cmake --build .  --target rocketjoe_worker

CMD ["./apps/rocketjoe_worker/rocketjoe_worker", "tasks.py"]

