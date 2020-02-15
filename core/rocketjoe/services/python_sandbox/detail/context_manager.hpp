#pragma once

#include <memory>
#include <list>
#include <unordered_map>

#include <boost/filesystem.hpp>
#include <boost/utility/string_view.hpp>

#include <rocketjoe/services/python_sandbox/detail/forward.hpp>

namespace rocketjoe { namespace services { namespace python_sandbox { namespace detail {

            constexpr const static  char* __default__ = "default" ;

            class context_manager final {
            public:
                context_manager(file_manager &file_manager);

                auto create_context(const std::string &name) -> context *;

                auto create_context() -> context *;

            private:
                file_manager &file_manager_;
                std::unordered_map <std::string, std::unique_ptr<context>> contexts_;
            };

}}}}