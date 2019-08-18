#pragma once

#include <pybind11/pybind11.h>
#include "data_set_manager.hpp"

namespace rocketjoe { namespace services { namespace python_engine {

    namespace py = pybind11;

    void add_mapreduce(py::module &pyrocketjoe, data_set_manager* dsm);

}}}