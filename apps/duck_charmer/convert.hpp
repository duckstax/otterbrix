#pragma once

#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "components/document/document.hpp"
#include "components/document/document_view.hpp"
#include <services/storage/sort.hpp>

namespace py = pybind11;

void to_document(const py::handle& source, components::document::document_t& target);
auto from_document(const components::document::document_view_t& document) -> py::object;

auto from_object(const components::document::document_view_t& document, const std::string& key) -> py::object;
auto from_object(const components::document::document_view_t& document, uint32_t index) -> py::object;

void update_document(const py::handle& source, components::document::document_t& target);

auto to_pylist(const std::vector<std::string> &src) -> py::list;

auto to_sorter(const py::handle &sort_dict) -> services::storage::sort::sorter_t;
auto to_order(const py::object &order) -> services::storage::sort::order;
