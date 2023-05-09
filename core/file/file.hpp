#pragma once

#include <vector>
#include <string>
#include <filesystem>

namespace core::file {

    using path_t = std::filesystem::path;

    class file_t {
    public:
        explicit file_t(const path_t &path);
        ~file_t();

        std::string read(std::size_t size, __off64_t offset = 0) const;
        std::string readline(__off64_t& offset, char delimer = '\n') const;
        std::string readall() const;
        void read(std::vector<char> &desc, std::size_t size, __off64_t offset = 0) const;
        void clear();
        void append(char* data, std::size_t size);
        void append(void* data, std::size_t size);
        void append(const void* data, std::size_t size);
        void append(std::string &data);
        void append(const std::string &data);
        void rewrite(std::string &data);
        void seek_eof();

    private:
        int fd_;
        __off64_t offset_;
    };

} //namespace core::file
