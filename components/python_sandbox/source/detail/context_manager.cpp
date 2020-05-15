#include <detail/context_manager.hpp>
#include <detail/context.hpp>
#include <detail/file_manager.hpp>
#include <detail/data_set.hpp>

namespace rocketjoe { namespace services { namespace python_sandbox { namespace detail {

                context_manager::context_manager(file_manager &file_manager) : file_manager_(file_manager) {}

                auto context_manager::create_context(const std::string &name) -> context * {
                    auto it = contexts_.emplace(name, std::make_unique<context>(file_manager_));
                    return it.first->second.get();
                }

                auto context_manager::create_context() -> context * {
                    return create_context(__default__);
                }

}}}}
