FROM alpine:latest

RUN apk update \
 && apk upgrade --no-cache \
 && apk --update add --no-cache \
        linux-headers \
        python3 \
        python3-dev \
        build-base \
        cmake \
        bash \
        git \
        lua \
        lua-dev \
        libstdc++ \
        py3-pip \
 && pip3 install --no-cache-dir conan \
 && rm -rf /var/cache/* \
 && rm -rf /root/.cache/*

COPY . /app
WORKDIR /app

RUN conan install .

WORKDIR /app/build
RUN cmake .. -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release
RUN cmake --build .

#ENTRYPOINT["bash"]
#CMD ["./script/entrypoint.sh"]
#ENTRYPOINT ["./entrypoint.sh"]
#CMD ["run"]