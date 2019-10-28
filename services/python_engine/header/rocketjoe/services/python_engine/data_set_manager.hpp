#pragma once

#include <unordered_map>
#include <functional>

#include <rocketjoe/services/python_engine/file_manager.hpp>


namespace rocketjoe { namespace services { namespace python_engine {

            class data_set final {
            private:
                using storage_t = std::map<std::size_t, std::string>;
                using const_iterator = storage_t::const_iterator;
            public:
                data_set(file_view*file):file_(file){}

                data_set():file_(nullptr){}

                auto file() -> file_view* {
                    return file_;
                }

            private:
                file_view*file_;
            };
}}}