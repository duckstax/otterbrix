#pragma once

#include <pybind11/pybind11.h>

namespace services { namespace interactive_python {

    namespace py = pybind11;

    class context_manager;

    auto add_jupyter(py::module&) -> void;

}} // namespace components::python::detail