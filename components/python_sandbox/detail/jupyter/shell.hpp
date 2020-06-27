#pragma once

#include <pybind11/embed.h>
#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace components { namespace detail { namespace jupyter {

    namespace py = pybind11;

    class shell final {
    public:
        static auto _default_banner1(py::object self) -> py::object;

        static auto _default_exiter(py::object self) -> py::object;

        static auto init_hooks(py::object self) -> void;

        static auto ask_exit(py::object self) -> void;

        static auto run_cell(py::object self, py::args args, py::kwargs kwargs)
          -> py::object;

        static auto _showtraceback(py::object self, py::object etype,
                                 py::object evalue, py::object stb) -> void;

        static auto set_next_input(py::object self, py::object text,
                                 py::bool_ replace = true) -> void;

        static auto set_parent(py::object self, py::dict parent) -> void;

        static auto init_environment(py::object self) -> void;

        static auto init_virtualenv(py::object self) -> void;
    };

}}}
