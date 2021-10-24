#pragma once

#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "components/document/document.hpp"

namespace py = pybind11;

void to_document(const py::handle& source, components::storage::document_t& target);
auto from_document(const components::storage::document_t& document) -> py::object;

auto from_object(const std::string& key, components::storage::document_t& target) -> py::object;

void update_document(const py::handle& source, components::storage::document_t& target);
