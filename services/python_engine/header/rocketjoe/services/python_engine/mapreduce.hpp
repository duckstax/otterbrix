#pragma once

#include <pybind11/pybind11.h>

namespace rocketjoe { namespace services { namespace python_engine {

    namespace py = pybind11;

    class context_manager;

    void add_mapreduce(py::module &pyrocketjoe, context_manager * dsm);

}}}