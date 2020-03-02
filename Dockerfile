FROM python:3.7-alpine

RUN apk update \
 && apk upgrade --no-cache \
 && apk --update add --no-cache \
        linux-headers \
        build-base \
        cmake \
        ninja \
        libstdc++ \
 && pip3 install --no-cache-dir conan \
 && rm -rf /var/cache/* \
 && rm -rf /root/.cache/*

COPY . /app
WORKDIR /app

RUN conan remote add jinncrafters https://api.bintray.com/conan/jinncrafters/conan
#RUN conan install . --build missing -s build_type=Release

WORKDIR /app/build
RUN cmake .. -GNinja -DCMAKE_BUILD_TYPE=Release
RUN cmake --build .

ENTRYPOINT["bash"]
CMD ["./script/entrypoint.sh"]
ENTRYPOINT ["./entrypoint.sh"]
CMD ["run"]