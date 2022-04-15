#pragma once

#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <pybind11/pybind11.h>
#include <components/document/document_id.hpp>

namespace py = pybind11;

namespace duck_charmer {

    class PYBIND11_EXPORT wrapper_document_id final : public boost::intrusive_ref_counter<wrapper_document_id> {

        using document_id_t = components::document::document_id_t;
        using time_value_t = document_id_t::time_value_t;

    public:
        wrapper_document_id();
        explicit wrapper_document_id(const std::string &str);
        explicit wrapper_document_id(time_value_t time);

        std::string value_of() const;
        std::string to_string() const;
        time_value_t get_timestamp() const;

    private:
        document_id_t id_;
    };

} // namespace duck_charmer
