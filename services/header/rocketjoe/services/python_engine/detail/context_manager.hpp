#pragma once

#include <memory>
#include <list>
#include <unordered_map>

#include <boost/filesystem.hpp>

#include <rocketjoe/services/python_engine/detail/forward.hpp>

namespace rocketjoe { namespace services { namespace python_engine { namespace detail {

            class context_manager final {
            public:
                context_manager(file_manager &file_manager);

                auto create_context(const std::string &name) -> context *;

            private:
                file_manager &file_manager_;
                std::unordered_map <std::string, std::unique_ptr<context>> contexts_;
            };

}}}}