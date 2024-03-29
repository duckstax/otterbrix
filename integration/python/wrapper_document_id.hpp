#pragma once

#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <components/document/document_id.hpp>
#include <pybind11/pybind11.h>

namespace py = pybind11;

namespace otterbrix {

    class PYBIND11_EXPORT wrapper_document_id final : public boost::intrusive_ref_counter<wrapper_document_id> {
        using document_id_t = components::document::document_id_t;

    public:
        wrapper_document_id();
        explicit wrapper_document_id(const std::string& str);
        explicit wrapper_document_id(oid::timestamp_value_t time);

        std::string value_of() const;
        std::string to_string() const;
        oid::timestamp_value_t get_timestamp() const;

    private:
        document_id_t id_;
    };

} // namespace otterbrix
