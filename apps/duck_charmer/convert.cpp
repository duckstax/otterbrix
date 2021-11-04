#include "convert.hpp"

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "nlohmann/json.hpp"

#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "components/document/core/dict.hpp"

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

using components::storage::document_t;
using components::storage::document_view_t;
using json = nlohmann::json;

inline json to_json(const py::handle& obj) {
    if (obj.ptr() == nullptr || obj.is_none()) {
        return nullptr;
    }
    if (py::isinstance<py::bool_>(obj)) {
        return obj.cast<bool>();
    }
    if (py::isinstance<py::int_>(obj)) {
        return obj.cast<long>();
    }
    if (py::isinstance<py::float_>(obj)) {
        return obj.cast<double>();
    }
    if (py::isinstance<py::bytes>(obj)) {
        py::module base64 = py::module::import("base64");
        return base64.attr("b64encode")(obj).attr("decode")("utf-8").cast<std::string>();
    }
    if (py::isinstance<py::str>(obj)) {
        return obj.cast<std::string>();
    }
    if (py::isinstance<py::tuple>(obj) || py::isinstance<py::list>(obj)) {
        auto out = json::array();
        for (const py::handle value : obj) {
            out.push_back(to_json(value));
        }
        return out;
    }
    if (py::isinstance<py::dict>(obj)) {
        auto out = json::object();
        for (const py::handle key : obj) {
            out[py::str(key).cast<std::string>()] = to_json(obj[key]);
        }
        return out;
    }
    throw std::runtime_error("to_json not implemented for this type of object: " + py::repr(obj).cast<std::string>());
}

inline ::storage::retained_const_t<::storage::impl::value_t> to__(const py::handle& obj) {
    if (py::isinstance<py::bool_>(obj)) {
        return ::storage::impl::new_value(obj.cast<bool>());
    }
    if (py::isinstance<py::int_>(obj)) {
        return ::storage::impl::new_value(obj.cast<long>());
    }
    if (py::isinstance<py::float_>(obj)) {
        return ::storage::impl::new_value(obj.cast<double>());
    }
    if (py::isinstance<py::bytes>(obj)) {
        py::module base64 = py::module::import("base64");
        return ::storage::impl::new_value(base64.attr("b64encode")(obj).attr("decode")("utf-8").cast<std::string>());
    }
    if (py::isinstance<py::str>(obj)) {
        return ::storage::impl::new_value(obj.cast<std::string>());
    }

    if (py::isinstance<py::tuple>(obj) || py::isinstance<py::list>(obj)) {
        auto out = document_t::create_array();
        for (const py::handle value : obj) {
            out->append(to__(value));
        }
        return out->as_array();
    }
    if (py::isinstance<py::dict>(obj)) {
        document_t out;
        to_document(obj, out);
        return out.value();
    }

    throw std::runtime_error("to_json not implemented for this type of object: " + py::repr(obj).cast<std::string>());
}

void to_document_inner(std::string&& key, const py::handle& source, document_t& target) {
    if (source.ptr() == nullptr || source.is_none()) {
        target.add_null(std::move(key));
        return;
    }
    if (py::isinstance<py::bool_>(source)) {
        target.add_bool(std::move(key), source.cast<bool>());
        return;
    }
    if (py::isinstance<py::int_>(source)) {
        target.add_long(std::move(key), source.cast<long>());
        return;
    }
    if (py::isinstance<py::float_>(source)) {
        target.add_double(std::move(key), source.cast<double>());
        return;
    }
    if (py::isinstance<py::bytes>(source)) {
        py::module base64 = py::module::import("base64");
        target.add_string(std::move(key), base64.attr("b64encode")(source).attr("decode")("utf-8").cast<std::string>());
        return;
    }
    if (py::isinstance<py::str>(source)) {
        target.add_string(std::move(key), source.cast<std::string>());
        return;
    }

    if (py::isinstance<py::tuple>(source) || py::isinstance<py::list>(source)) {
        auto inner_doc = document_t::create_array();
        for (const py::handle value : source) {
            inner_doc->append(to__(value));
        }
        target.add_array(std::move(key), inner_doc);
        return;
    }
    if (py::isinstance<py::dict>(source)) {
        document_t inner_doc;
        to_document(source, inner_doc);
        target.add_dict(std::move(key), std::move(inner_doc));
        return;
    }

    throw std::runtime_error("to_document not implemented for this type of object: " + py::repr(source).cast<std::string>());
}

void to_document(const py::handle& source, document_t& target) {
    for (const py::handle key : source) {
        to_document_inner(py::str(key).cast<std::string>(), source[key], target);
    }
}

void update_document_inner(std::string&& key, const py::handle& obj, document_t& target) {
    if (obj.ptr() == nullptr || obj.is_none()) {
        target.add_null(key);
        return;
    }
    if (py::isinstance<py::bool_>(obj)) {
        target.add_bool(key, obj.cast<bool>());
        return;
    }
    if (py::isinstance<py::int_>(obj)) {
        target.add_long(key, obj.cast<long>());
        return;
    }
    if (py::isinstance<py::float_>(obj)) {
        target.add_double(key, obj.cast<double>());
        return;
    }
    if (py::isinstance<py::bytes>(obj)) {
        py::module base64 = py::module::import("base64");
        target.add_string(key, base64.attr("b64encode")(obj).attr("decode")("utf-8").cast<std::string>());
    }
    if (py::isinstance<py::str>(obj)) {
        target.add_string(key, obj.cast<std::string>());
        return;
    }
    /*
    if (py::isinstance<py::tuple>(obj) || py::isinstance<py::list>(obj)) {
        auto out = nl::json::array();
        for (const py::handle value : obj) {
            out.push_back(to_json(value));
        }
        return out;
    }
    if (py::isinstance<py::dict>(obj)) {
        auto out = nl::json::object();
        for (const py::handle key : obj) {
            out[py::str(key).cast<std::string>()] = to_json(obj[key]);
        }
        return out;
    }
     */
    throw std::runtime_error("update_document_inner not implemented for this type of object: " + py::repr(obj).cast<std::string>());
}

auto from_document(const document_view_t &document) -> py::object {
    if (document.is_dict()) {
        py::dict dict;
        for (auto it = document.begin(); it; ++it) {
            auto key = static_cast<std::string>(it.key()->as_string());
            if (document.is_null(std::string(key))) {
                dict[py::str(key)] = py::none();
            } else if (document.is_bool(std::string(key))) {
                dict[py::str(key)] = py::bool_(document.get_as<bool>(key));
            } else if (document.is_ulong(std::string(key))) {
                dict[py::str(key)] = py::int_(document.get_as<ulong>(key));
            } else if (document.is_long(std::string(key))) {
                dict[py::str(key)] = py::int_(document.get_as<long>(key));
            } else if (document.is_float(std::string(key))) {
                dict[py::str(key)] = py::float_(document.get_as<float>(key));
            } else if (document.is_double(std::string(key))) {
                dict[py::str(key)] = py::float_(document.get_as<double>(key));
            } else if (document.is_string(std::string(key))) {
                dict[py::str(key)] = py::str(document.get_as<std::string>(key));
            } else if (document.is_array(std::string(key))) {
                dict[py::str(key)] = from_document(document.get_array(std::string(key)));
            } else if (document.is_dict(std::string(key))) {
                dict[py::str(key)] = from_document(document.get_dict(std::string(key)));
            } else {
                dict[py::str(key)] = py::none();
            }
        }
        return std::move(dict);
    } else {
        py::list list;
        for (uint32_t i = 0; i < document.count(); ++i) {
            if (document.is_null(i)) {
                list.append(py::none());
            } else if (document.is_bool(i)) {
                list.append(py::bool_(document.get_as<bool>(i)));
            } else if (document.is_ulong(i)) {
                list.append(py::int_(document.get_as<ulong>(i)));
            } else if (document.is_long(i)) {
                list.append(py::int_(document.get_as<long>(i)));
            } else if (document.is_float(i)) {
                list.append(py::float_(document.get_as<float>(i)));
            } else if (document.is_double(i)) {
                list.append(py::float_(document.get_as<double>(i)));
            } else if (document.is_string(i)) {
                list.append(py::str(document.get_as<std::string>(i)));
            } else if (document.is_array(i)) {
                list.append(from_document(document.get_array(i)));
            } else if (document.is_dict(i)) {
                list.append(from_document(document.get_dict(i)));
            } else {
                list.append(py::none());
            }
        }
        return std::move(list);
    }
    return py::none();
}

auto from_object(const std::string& key, document_t& target) -> py::object {
//    auto value = target.get(key);
//    return from_document(value);
}

void update_document(const py::handle& source, document_t& target) {
    for (const py::handle key : source) {
        //std::cerr << py::str(key).cast<std::string>()  << std::endl;
        update_document_inner(py::str(key).cast<std::string>(), source[key], target);
    }

    ///throw std::runtime_error("update_document not implemented for this type of object: " + py::repr(source).cast<std::string>());
}
