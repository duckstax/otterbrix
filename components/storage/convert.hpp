#pragma once

#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "friedrichdb/core/document.hpp"

namespace py = pybind11;

void to_document(const py::handle &source, friedrichdb::core::document_t& target);

auto  from_object(const std::string& key, friedrichdb::core::document_t& target) -> py::object ;

void  update_document(const py::handle &source, friedrichdb::core::document_t& target) ;