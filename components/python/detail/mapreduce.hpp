#pragma once

#include <pybind11/pybind11.h>

namespace components { namespace python { namespace detail {

    namespace py = pybind11;

    class context_manager;

    auto add_mapreduce(py::module&, context_manager*) -> void;

}}} // namespace components::python::detail