#include "wrapper_document_id.hpp"
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <pybind11/stl_bind.h>

PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

namespace duck_charmer {

    wrapper_document_id::wrapper_document_id()
        : id_(document_id_t()) {}

    wrapper_document_id::wrapper_document_id(const std::string& str)
        : id_(str) {}

    wrapper_document_id::wrapper_document_id(oid::timestamp_value_t time)
        : id_(document_id_t(time)) {}

    std::string wrapper_document_id::value_of() const {
        return id_.to_string();
    }

    std::string wrapper_document_id::to_string() const {
        return std::string("ObjectId(\"") + id_.to_string() + std::string("\")");
    }

    oid::timestamp_value_t wrapper_document_id::get_timestamp() const {
        return id_.get_timestamp();
    }

} // namespace python
