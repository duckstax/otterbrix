#include "convert.hpp"

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "nlohmann/json.hpp"

#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "components/document/mutable/mutable_dict.h"
#include "components/document/mutable/mutable_array.h"

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

using components::document::document_ptr;
using components::document::document_view_t;

inline ::document::retained_const_t<::document::impl::value_t> to_(const py::handle& obj) {
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

auto to_document(const py::handle& source) -> components::document::document_ptr {
    return components::document::make_document(to_(source)->as_dict());
}

auto from_document(const document_view_t &document) -> py::object {
    if (document.is_dict()) {
        py::dict dict;
        for (auto it = document.begin(); it; ++it) {
            auto key = static_cast<std::string>(it.key()->as_string());
            dict[py::str(key)] = from_object(document, key);
        }
        return std::move(dict);
    } else {
        py::list list;
        for (uint32_t i = 0; i < document.count(); ++i) {
            list.append(from_object(document, i));
        }
        return std::move(list);
    }
    return py::none();
}

template <class TKey>
auto from_object_(const document_view_t &document, const TKey &key) -> py::object {
    if (document.is_null(key)) {
        return py::none();
    } else if (document.is_bool(key)) {
        return py::bool_(document.get_as<bool>(key));
    } else if (document.is_ulong(key)) {
        return py::int_(document.get_as<ulong>(key));
    } else if (document.is_long(key)) {
        return py::int_(document.get_as<long>(key));
    } else if (document.is_float(key)) {
        return py::float_(document.get_as<float>(key));
    } else if (document.is_double(key)) {
        return py::float_(document.get_as<double>(key));
    } else if (document.is_string(key)) {
        return py::str(document.get_as<std::string>(key));
    } else if (document.is_array(key)) {
        py::list list;
        auto array = document.get_array(key);
        for (uint32_t i = 0; i < array.count(); ++i) {
            list.append(from_object_<uint32_t>(array, i));
        }
        return std::move(list);
    } else if (document.is_dict(key)) {
        py::dict dict;
        auto sub_doc = document.get_dict(key);
        for (auto it = sub_doc.begin(); it; ++it) {
            auto key = static_cast<std::string>(it.key()->as_string());
            dict[py::str(key)] = from_object_<std::string>(sub_doc, key);
        }
        return std::move(dict);
    }
    return py::none();
}

auto from_object(const document_view_t &document, const std::string &key) -> py::object {
    return from_object_<std::string>(document, std::string(key));
}

auto from_object(const document_view_t &document, uint32_t index) -> py::object {
    return from_object_<uint32_t>(document, index);
}

auto to_pylist(const std::vector<std::string> &src) -> py::list {
    py::list res;
    for (const auto &str : src) {
        res.append(str);
    }
    return res;
}

auto to_pylist(const std::vector<components::document::document_id_t>& src) -> py::list {
    py::list res;
    for (const auto &str : src) {
        res.append(str.to_string());
    }
    return res;
}

auto to_sorter(const py::handle &sort_dict) -> services::storage::sort::sorter_t {
    services::storage::sort::sorter_t sorter;
    for (const py::handle key : sort_dict) {
        sorter.add(py::str(key).cast<std::string>(), to_order(sort_dict[key]));
    }
    return sorter;
}

auto to_order(const py::object &order) -> services::storage::sort::order {
    return py::int_(order).cast<int>() < 0
            ? services::storage::sort::order::descending
            : services::storage::sort::order::ascending;
}
