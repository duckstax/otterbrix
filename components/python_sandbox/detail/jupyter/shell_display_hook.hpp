#pragma once

#include <pybind11/pybind11.h>

namespace rocketjoe { namespace services { namespace detail { namespace jupyter {

    namespace py = pybind11;

    class shell_display_hook final {
    public:
        static auto set_parent(py::object self, py::dict parent) -> void;

        static auto start_displayhook(py::object self) -> void;

        static auto write_output_prompt(py::object self) -> void;

        static auto write_format_data(py::object self, py::dict data,
                                    py::dict metadata) -> void;

        static auto finish_displayhook(py::object self) -> void;
    };

}}}}
