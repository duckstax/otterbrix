#include "convert.hpp"

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "nlohmann/json.hpp"

#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

using friedrichdb::core::document_t;
using json = nlohmann::json;


inline json to_json(const py::handle &obj) {
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

inline document_t to__(const py::handle &obj) {
    /*
    if (obj.ptr() == nullptr || obj.is_none()) {
        return nullptr;
    }
     */
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
            out.emplace_back(to_json(value));
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

void to_document( const py::handle &source, friedrichdb::core::document_t& target);

void to_document_inner(std::string&& key, const py::handle &source, friedrichdb::core::document_t& target) {
    if (source.ptr() == nullptr || source.is_none()) {
        target.add(std::move(key));
        return;
    }
    if (py::isinstance<py::bool_>(source)) {
        target.add(std::move(key),source.cast<bool>());
        return;
    }
    if (py::isinstance<py::int_>(source)) {
        ///std::cerr << "0 to_document_inner int" << std::endl;
        target.add(std::move(key),source.cast<long>());
        return;
    }
    if (py::isinstance<py::float_>(source)) {
        target.add(std::move(key),source.cast<double>());
        return ;
    }
    if (py::isinstance<py::bytes>(source)) {
        py::module base64 = py::module::import("base64");
        target.add(std::move(key),base64.attr("b64encode")(source).attr("decode")("utf-8").cast<std::string>());
        return ;
    }
    if (py::isinstance<py::str>(source)) {
        target.add(std::move(key),source.cast<std::string>());
        return;
    }


    if (py::isinstance<py::tuple>(source) || py::isinstance<py::list>(source)) {
        auto inner_doc = friedrichdb::core::document_t::to_array() ;
        for (const py::handle value : source) {
            inner_doc.append(to__(value));
        }
        target.add(std::move(key),std::move(inner_doc));
        return ;
    }
    if (py::isinstance<py::dict>(source)) {
        friedrichdb::core::document_t inner_doc;
        to_document(source,inner_doc);
        target.add(std::move(key),std::move(inner_doc));
        return ;
    }

    throw std::runtime_error("to_document not implemented for this type of object: " + py::repr(source).cast<std::string>());
}

void to_document( const py::handle &source, friedrichdb::core::document_t& target) {
    for (const py::handle key : source) {
        to_document_inner(py::str(key).cast<std::string>(), source[key], target);
    }
}

void update_document_inner(std::string&& key, const py::handle &obj,friedrichdb::core::document_t& target) {

    if (obj.ptr() == nullptr || obj.is_none()) {
        target.update(key);
        return ;
    }
    if (py::isinstance<py::bool_>(obj)) {
        target.update(key,obj.cast<bool>());
        return ;
    }
    if (py::isinstance<py::int_>(obj)) {
        target.update(key,obj.cast<long>());
        return ;
    }
    if (py::isinstance<py::float_>(obj)) {
        target.update(key,obj.cast<double>());
        return ;
    }
    if (py::isinstance<py::bytes>(obj)) {
        py::module base64 = py::module::import("base64");
        target.update(key, base64.attr("b64encode")(obj).attr("decode")("utf-8").cast<std::string>());
    }
    if (py::isinstance<py::str>(obj)) {
        target.update(key,obj.cast<std::string>());
        return ;
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


auto  from_object(const std::string& key, friedrichdb::core::document_t& target) -> py::object {
    auto value = target.get(key);
    if (value.is_null()) {
        return py::none();
    } else if (value.is_boolean()) {
        return py::bool_(value.as_bool());
    } else if (value.is_integer()) {
        return py::int_(value.as_int32());
    } else if (value.is_float()) {
        return py::float_(value.as_double());
    }  else if (value.is_string()) {
        return py::str(value.as_string());
    } /*else if (target.get(key).is_array()) {
        py::list obj;
        auto&j = target.get(key);
        for (const auto &el : j) {
            obj.append(from_json(el));
        }
        return std::move(obj);
    } else // Object
    {
        py::dict obj;
        for (nl::json::const_iterator it = j.cbegin(); it != j.cend(); ++it) {
            obj[py::str(it.key())] = from_json(it.value());
        }
        return std::move(obj);
    }
    */
}

void update_document( const py::handle &source, friedrichdb::core::document_t &target) {

    for (const py::handle key : source) {
        //std::cerr << py::str(key).cast<std::string>()  << std::endl;
        update_document_inner(py::str(key).cast<std::string>(), source[key],target);

    }

    ///throw std::runtime_error("update_document not implemented for this type of object: " + py::repr(source).cast<std::string>());
}
