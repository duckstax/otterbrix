#pragma once

#include <memory>
#include <list>
#include <unordered_map>

#include <boost/filesystem.hpp>

#include <rocketjoe/services/python_sandbox/detail/forward.hpp>

namespace rocketjoe { namespace services { namespace python_sandbox { namespace detail {

                class context final {
                public:
                    context(file_manager &file_manager);

                    auto text_file(const std::string &path) -> data_set;


                private:
                    file_manager &file_manager_;
                    std::list<std::unique_ptr<data_set>> pipeline_;
                };

}}}}