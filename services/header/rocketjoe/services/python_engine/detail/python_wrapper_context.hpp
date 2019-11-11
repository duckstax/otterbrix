#pragma once

#include <string>

#include <rocketjoe/services/python_engine/detail/forward.hpp>

namespace rocketjoe { namespace services { namespace python_engine { namespace detail {

                class context_wrapper final {
                public:
                    context_wrapper(const std::string &name, context *ctx);

                    auto text_file(const std::string &path) -> python_wrapper_data_set;

                private:
                    std::string name_;
                    context *ctx_;
                };

}}}}