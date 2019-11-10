#pragma once

#include <memory>
#include <stack>
#include <unordered_map>

#include <boost/filesystem.hpp>

#include <rocketjoe/services/python_engine/detail/forward.hpp>

namespace rocketjoe { namespace services { namespace python_engine { namespace detail {

            class context final {
            public:
                context(file_manager &file_manager);

                auto top() -> data_set *;

                auto open_file(const boost::filesystem::path& ) -> file_view* ;

                auto next() -> data_set*;


            private:
                file_manager &file_manager_;
                std::stack <std::unique_ptr<data_set>> archive_data_set_;
            };


            class context_manager final {
            public:
                context_manager(file_manager &file_manager);

                auto create_context(const std::string &name) -> context *;

            private:
                file_manager &file_manager_;
                std::unordered_map <std::string, std::unique_ptr<context>> contexts_;
            };

}}}}