#pragma once

#include <list>
#include <memory>
#include <unordered_map>

#include <boost/filesystem.hpp>
#include <boost/utility/string_view.hpp>

#include <detail/forward.hpp>

namespace components { namespace python { namespace detail {

    constexpr const static char* __default__ = "default";

    class context_manager final {
    public:
        context_manager(file_manager& file_manager);

        auto create_context(const std::string& name) -> context*;

        auto create_context() -> context*;

    private:
        file_manager& file_manager_;
        std::unordered_map<std::string, std::unique_ptr<context>> contexts_;
    };

}}} // namespace components::python::detail