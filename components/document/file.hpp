#pragma once
#include <string>
#include <boost/filesystem.hpp>

namespace components::file {

    using path_t = boost::filesystem::path;

    void write(const path_t &path, std::string &data);
    void append(const path_t &path, std::string &data);
    std::string readall(const path_t &path);

} //namespace components::file
