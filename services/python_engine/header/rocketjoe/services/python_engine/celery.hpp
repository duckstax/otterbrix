#pragma once

#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include <pybind11/pybind11.h>

namespace rocketjoe { namespace services { namespace python_engine {

            using namespace pybind11::literals;
            namespace py = pybind11;
            using py_task = py::function;

            void add_celery(py::module &pyrocketjoe);

}}}