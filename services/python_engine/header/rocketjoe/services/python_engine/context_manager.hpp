#pragma once

#include <rocketjoe/services/python_engine/forward.hpp>
#include <rocketjoe/services/python_engine/file_manager.hpp>

namespace rocketjoe { namespace services { namespace python_engine {

            class context final {
            public:
                context(file_manager &file_manager) : file_manager_(file_manager) {}

                auto top() -> data_set *;

                auto open_file(const boost::filesystem::path& path);

                auto next() -> data_set*;


                auto make_new_version_data_set

            private:
                file_manager &file_manager_;
                std::stack <std::unique_ptr<data_set>> archive_data_set_;
            };


            class context_manager final {
            public:
                context_manager(file_manager &file_manager): file_manager_(file_manager) {}

                auto create_context(const std::string &name) -> context * {
                    auto it =  contexts_.emplace(name,std::make_unique<context>(file_manager_));
                    return it.first->second.get();
                }

            private:
                file_manager &file_manager_;
                std::unordered_map <std::string, std::unique_ptr<context>> contexts_;
            };

}}}