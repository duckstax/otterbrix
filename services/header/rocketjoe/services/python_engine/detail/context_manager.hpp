#pragma once

#include <memory>
#include <list>
#include <unordered_map>

#include <boost/filesystem.hpp>
#include <boost/utility/string_view.hpp>

#include <rocketjoe/services/python_engine/detail/forward.hpp>

namespace rocketjoe { namespace services { namespace python_engine { namespace detail {

            constexpr static  char* __default__ = "default" ;

            class context_manager final {
            public:
                context_manager(file_manager &file_manager);

                auto create_context(const std::string &name) -> mapreduce_context *;
                auto create_context() -> mapreduce_context *;

            private:
                file_manager &file_manager_;
                std::unordered_map <std::string, std::unique_ptr<mapreduce_context>> contexts_;
            };

}}}}