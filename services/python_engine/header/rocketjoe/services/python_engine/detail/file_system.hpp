#pragma once

#include <pybind11/pybind11.h>

namespace rocketjoe { namespace services { namespace python_engine { namespace detail {

            namespace py = pybind11;

            class file_manager;

            auto add_file_system(py::module &, file_manager *) -> void;

}}} }