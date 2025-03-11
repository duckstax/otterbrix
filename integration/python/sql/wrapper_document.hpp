#pragma once

#include <memory>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include <components/document/document.hpp>

namespace py = pybind11;
namespace otterbrix {
    class PYBIND11_EXPORT wrapper_document final : public boost::intrusive_ref_counter<wrapper_document> {
    public:
        explicit wrapper_document(components::document::document_ptr ptr);
        ~wrapper_document() = default;
        std::string print();
        py::object get(const std::string& key);

    private:
        components::document::document_ptr ptr_;
    };

    using wrapper_document_ptr = boost::intrusive_ptr<wrapper_document>;
} // namespace otterbrix