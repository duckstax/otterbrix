#pragma once

#include <rocketjoe/services/python_engine/data_set_manager.hpp>

namespace rocketjoe { namespace services { namespace python_engine {

            class context final {
            public:
                context(file_manager &file_manager) : end(0),file_manager_(file_manager) {}

                auto id() -> std::size_t {
                    return end;
                }

                auto get(std::size_t index)-> data_set* {
                    return archive_data_set_.at(index).get();
                }

                auto top() -> data_set* {
                    return archive_data_set_.at(end).get();
                }

                auto read_file(boost::filesystem::path path) {
                    auto* file = file_manager_.open(path);
                    archive_data_set_.emplace(end,std::make_unique<data_set>(file));
                    ++end;
                }

                auto next() -> data_set* {
                    auto result = archive_data_set_.emplace(archive_data_set_.size(),std::make_unique<data_set>());
                    ++end;
                    return result.first->second.get();
                }

            private:
                std::size_t end;
                file_manager &file_manager_;
                std::unordered_map <std::size_t , std::unique_ptr<data_set>> archive_data_set_;
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