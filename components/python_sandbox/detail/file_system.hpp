#pragma once

#include <pybind11/pybind11.h>

namespace components { namespace python_sandbox { namespace detail {

    namespace py = pybind11;

    class file_manager;

    auto add_file_system(py::module&, file_manager*) -> void;

}}} // namespace components::python_sandbox::detail