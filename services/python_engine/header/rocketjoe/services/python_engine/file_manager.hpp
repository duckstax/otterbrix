#pragma  once

#include <unordered_map>
#include <fstream>
#include <iostream>
#include <map>

#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/filesystem.hpp>
#include <boost/utility/string_view.hpp>

namespace rocketjoe { namespace services { namespace python_engine {

            class file_view final {
            private:
                using storage_t = std::map<std::size_t, std::string>;
            public:
                explicit file_view(const boost::filesystem::path &path);

                auto read() -> storage_t;

            private:
                boost::interprocess::file_mapping file__;
                boost::interprocess::mapped_region region;
                boost::string_view raw_;
                storage_t file_content;
            };

            class file_manager final {
            public:
                auto open(const std::string &path) -> file_view & {
                    file_view tmp(path);
                    auto result = files.emplace(path, std::move(tmp));
                    return result.first->second;
                }

            private:
                std::unordered_map<std::string, file_view> files;
            };

}}}