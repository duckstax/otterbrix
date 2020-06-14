#pragma once

#include <pybind11/embed.h>
#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace components { namespace detail { namespace jupyter {

    namespace py = pybind11;

    class zmq_ostream final {
    public:
        static auto set_parent(py::object self, py::dict parent) -> void;

        static auto writable(py::object self) -> bool;

        static auto write(py::object self, py::str string) -> void;

        static auto flush(py::object self) -> void;
    };

}}}
