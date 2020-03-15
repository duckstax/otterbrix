#pragma once

#include <pybind11/embed.h>
#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace rocketjoe { namespace services { namespace detail { namespace jupyter {

namespace py = pybind11;

class display_publisher final {
public:
  static auto set_parent(py::object self, py::dict parent) -> void;

  static auto publish(py::object self, py::dict data, py::dict metadata,
                      py::str source, py::dict trasistent, py::bool_ update)
      -> void;

  static auto clear_output(py::object self, py::bool_ wait = false) -> void;
};

}}}}