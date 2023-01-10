#pragma once

#include <memory_resource>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include <components/document/document.hpp>
#include <services/collection/sort.hpp>

#include "ql/aggregate.hpp"

namespace py = pybind11;

auto to_document(const py::handle& source) -> components::document::document_ptr;
auto from_document(const components::document::document_view_t& document) -> py::object;

auto from_object(const components::document::document_view_t& document, const std::string& key) -> py::object;
auto from_object(const components::document::document_view_t& document, uint32_t index) -> py::object;

auto to_pylist(const std::pmr::vector<std::string> &src) -> py::list;
auto to_pylist(const std::pmr::vector<components::document::document_id_t> &src) -> py::list;

auto to_sorter(const py::handle &sort_dict) -> services::storage::sort::sorter_t;
auto to_order(const py::object &order) -> services::storage::sort::order;

auto to_statement(const py::handle& source, components::ql::aggregate_statement*) -> void;
auto test_to_statement(const py::handle& source) -> py::str;

auto pack_to_match(const py::object &object) -> py::list;
