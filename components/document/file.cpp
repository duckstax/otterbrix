#include "file.hpp"
#include <fcntl.h>
#include <sys/uio.h>

namespace components::file {

    file_t::file_t(const path_t &path)
        : fd_(::open(path.c_str(), O_CREAT | O_RDWR, 0777))
        , offset_(0) {
    }

    file_t::~file_t() {
        ::close(fd_);
    }

    std::string file_t::read(std::size_t size, __off64_t offset) const {
        std::string data(size, '\0');
        auto size_read = ::pread(fd_, &data[0], size, offset);
        if (size_read < 0) {
            return std::string();
        }
        data.resize(size_t(size_read));
        return data;
    }

    std::string file_t::readall() const {
        constexpr std::size_t size_buffer = 1024;
        __off64_t pos = 0;
        std::string data;
        std::string buffer = read(size_buffer);
        while (!buffer.empty()) {
            data += buffer;
            pos += __off64_t(buffer.size());
            buffer = read(size_buffer, pos);
        }
        return data;
    }

    void file_t::read(std::vector<char> &desc, ::size_t size, __off64_t offset) const {
        desc.resize(size);
        ::pread(fd_, desc.data(), size, offset);
    }

    void file_t::clear() {
        offset_ = 0;
        ::ftruncate(fd_, offset_);
    }

    void file_t::append(char *data, std::size_t size) {
        iovec write_data{data, size};
        offset_ += ::pwritev(fd_, &write_data, 1, offset_);
    }

    void file_t::append(std::string &data) {
        append(data.data(), data.size());
    }

    void file_t::rewrite(std::string& data) {
        offset_ = 0;
        append(data);
        ::ftruncate(fd_, offset_);
    }

} //namespace components::file
