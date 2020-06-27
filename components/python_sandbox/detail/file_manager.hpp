#pragma once

#include <fstream>
#include <iostream>
#include <unordered_map>

#include <boost/filesystem.hpp>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/utility/string_view.hpp>

namespace components { namespace python_sandbox { namespace detail {

    class file_view final {
    private:
        using storage_t = std::unordered_map<std::size_t, std::string>;

    public:
        file_view(const boost::filesystem::path& path);

        auto read() -> storage_t;

        auto raw_file() -> boost::string_view& {
            return raw_;
        }

    private:
        boost::interprocess::file_mapping file__;
        boost::interprocess::mapped_region region;
        boost::string_view raw_;
        storage_t file_content; ///TODO: OLD
    };

    class file_manager final {
    public:
        auto open(const boost::filesystem::path& path) -> file_view*;

    private:
        std::unordered_map<std::string, std::unique_ptr<file_view>> files_;
    };

}}} // namespace components::python_sandbox::detail