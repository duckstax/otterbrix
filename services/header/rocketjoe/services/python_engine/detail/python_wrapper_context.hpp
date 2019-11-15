#pragma once

#include <string>

#include <rocketjoe/services/python_engine/detail/forward.hpp>

namespace rocketjoe { namespace services { namespace python_engine { namespace detail {

                class python_wrapper_context final {
                public:
                    python_wrapper_context(const std::string &name, mapreduce_context *ctx);

                    explicit python_wrapper_context(mapreduce_context *ctx);

                    auto text_file(const std::string &path) -> python_wrapper_data_set;

                private:
                    std::string name_;
                    mapreduce_context *ctx_;
                };

}}}}