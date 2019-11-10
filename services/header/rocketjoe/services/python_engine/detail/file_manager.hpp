#pragma  once

#include <unordered_map>
#include <fstream>
#include <iostream>
#include <map>

#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/filesystem.hpp>
#include <boost/utility/string_view.hpp>

namespace rocketjoe { namespace services { namespace python_engine { namespace detail {

            class file_view final {
            private:
                using storage_t = std::map<std::size_t, std::string>;
            public:
                file_view(const boost::filesystem::path &path);

                auto read() -> storage_t;

                auto raw_file() -> boost::string_view& {
                    return  raw_;
                }

            private:
                boost::interprocess::file_mapping file__;
                boost::interprocess::mapped_region region;
                boost::string_view raw_;
                storage_t file_content; ///TODO: OLD
            };

            class file_manager final {
            public:
                auto open(const boost::filesystem::path &path) -> file_view * {
                    if (boost::filesystem::exists(path)) {
                        auto it = files_.find(path.string());
                        if (it == files_.end()) {
                            auto result = files_.emplace(path.string(), std::make_unique<file_view>(path));
                            return result.first->second.get();
                        } else {
                            return it->second.get();
                        }
                    } else {
                        return nullptr;
                    }
                }

            private:
                std::unordered_map<std::string, std::unique_ptr<file_view>> files_;
            };

}}}}