#include "file.hpp"
#include <fcntl.h>
#include <sys/uio.h>

namespace components::file {

    void write(const path_t &path, std::string &data) {
        auto f = ::open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0777);
        iovec write_data{data.data(), data.size()};
        ::pwritev(f, &write_data, 1, 0);
        ::close(f);
    }

    void append(const path_t &path, std::string &data) {
        //todo test
        auto f = ::open(path.c_str(), O_CREAT | O_APPEND, 0777);
        iovec write_data{data.data(), data.size()};
        ::pwritev(f, &write_data, 1, 0);
        ::close(f);
    }

    std::string readall(const path_t &path) {
        constexpr std::size_t size_buffer = 1024;
        auto file = ::open(path.c_str(), O_RDONLY, 0777);
        __off_t pos = 0;
        std::string data;
        char buffer[size_buffer];
        auto size_read = ::pread(file, &buffer, size_buffer, pos);
        while (size_read > 0) {
            data += std::string(buffer, std::size_t(size_read));
            pos += size_read;
            size_read = ::pread(file, &buffer, size_buffer, pos);
        }
        ::close(file);
        return data;
    }

} //namespace components::file
