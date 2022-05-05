#pragma once
#include <string>
#include <boost/filesystem.hpp>

namespace components::file {

    using path_t = boost::filesystem::path;

    class file_t {
    public:
        explicit file_t(const path_t &path);
        ~file_t();

        std::string read(std::size_t size, __off64_t offset = 0) const;
        std::string readall() const;
        void read(std::vector<char> &desc, std::size_t size, __off64_t offset = 0) const;
        void clear();
        void append(char *data, std::size_t size);
        void append(std::string &data);
        void rewrite(std::string &data);

    private:
        int fd_;
        __off64_t offset_;
    };

} //namespace components::file
