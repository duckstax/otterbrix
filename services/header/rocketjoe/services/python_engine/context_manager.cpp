#include <rocketjoe/services/python_engine/detail/context_manager.hpp>
#include <rocketjoe/services/python_engine/detail/mapreduce_context.hpp>
#include <rocketjoe/services/python_engine/detail/data_set_manager.hpp>
#include <rocketjoe/services/python_engine/detail/file_manager.hpp>

namespace rocketjoe { namespace services { namespace python_engine { namespace detail {

                auto mapreduce_context::next() -> data_set * {
                    pipeline_.emplace_back(std::make_unique<data_set>());
                    return pipeline_.front().get();
                }

                auto mapreduce_context::open_file(const boost::filesystem::path &path) -> file_view* {
                    return file_manager_.open(path);
                }

                mapreduce_context::mapreduce_context(file_manager &file_manager) : file_manager_(file_manager) {}

                context_manager::context_manager(file_manager &file_manager) : file_manager_(file_manager) {}

                auto context_manager::create_context(const std::string &name) -> mapreduce_context * {
                    auto it = contexts_.emplace(name, std::make_unique<mapreduce_context>(file_manager_));
                    return it.first->second.get();
                }

                auto context_manager::create_context() -> mapreduce_context * {
                    return create_context(__default__);
                }

}}}}