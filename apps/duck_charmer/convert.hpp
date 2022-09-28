#pragma once

#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "components/document/document.hpp"
#include "components/document/document_view.hpp"
#include "components/document/document_id.hpp"
#include <services/collection/sort.hpp>

#include <components/ql/aggregate.hpp>

namespace py = pybind11;

auto to_document(const py::handle& source) -> components::document::document_ptr;
auto from_document(const components::document::document_view_t& document) -> py::object;

auto from_object(const components::document::document_view_t& document, const std::string& key) -> py::object;
auto from_object(const components::document::document_view_t& document, uint32_t index) -> py::object;

auto to_pylist(const std::vector<std::string> &src) -> py::list;
auto to_pylist(const std::vector<components::document::document_id_t> &src) -> py::list;

auto to_sorter(const py::handle &sort_dict) -> services::storage::sort::sorter_t;
auto to_order(const py::object &order) -> services::storage::sort::order;

namespace experimental {

    ::document::retained_const_t<::document::impl::value_t> to_v2_(const py::handle& obj) {
        if (py::isinstance<py::bool_>(obj)) {
            return ::document::impl::new_value(obj.cast<bool>());
        }
        if (py::isinstance<py::int_>(obj)) {
            return ::document::impl::new_value(obj.cast<long>());
        }
        if (py::isinstance<py::float_>(obj)) {
            return ::document::impl::new_value(obj.cast<double>());
        }
        if (py::isinstance<py::bytes>(obj)) {
            py::module base64 = py::module::import("base64");
            return ::document::impl::new_value(base64.attr("b64encode")(obj).attr("decode")("utf-8").cast<std::string>());
        }
        if (py::isinstance<py::str>(obj)) {
            return ::document::impl::new_value(obj.cast<std::string>());
        }
        if (py::isinstance<py::tuple>(obj) || py::isinstance<py::list>(obj)) {
            auto out = ::document::impl::mutable_array_t::new_array();
            for (const py::handle value : obj) {
                out->append(to_(value));
            }
            return out->as_array();
        }
        if (py::isinstance<py::dict>(obj)) {
            auto out = ::document::impl::mutable_dict_t::new_dict();
            for (const py::handle key : obj) {
                out->set(py::str(key).cast<std::string>(), to_(obj[key]));
            }
            return out->as_dict();
        }
    }

    auto to_statement(const py::handle& source) -> components::ql::aggregate_statement {
        auto is_list = py::isinstance<py::list>(source);
        auto is_dict = py::isinstance<py::dict>(source);
        if (is_list || is_dict) {
            auto size = py::len(source);
            components::ql::aggregate_statement aggregate;

            if (is_list) {
            }

            if (is_dict) {
                for (const py::handle key : source) {
                    out->set(py::str(key).cast<std::string>(), to_v2_(source[key]));
                }
            }
            return aggregate;
        }
    }

    auto from_statement(const components::document::document_view_t& document) -> py::object {

    }

}