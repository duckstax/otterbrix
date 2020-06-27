#include <detail/context.hpp>

#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <detail/context.hpp>
#include <detail/data_set.hpp>
#include <detail/file_manager.hpp>

namespace components { namespace python_sandbox { namespace detail {

    using namespace pybind11::literals;
    namespace py = pybind11;

    auto context::text_file(const std::string& path) -> boost::intrusive_ptr<data_set> {
        auto* file = file_manager_.open(path);
        auto all_file = file->raw_file();
        return make_new_step<data_set>(py::str(all_file.to_string()), this);
    }

    context::context(file_manager& file_manager)
        : file_manager_(file_manager) {}

}}} // namespace components::python_sandbox::detail
