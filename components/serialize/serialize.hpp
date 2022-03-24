#pragma once
#include <components/document/document.hpp>

namespace components::serialize {

    using components::document::document_t;
    using components::document::document_ptr;

    struct serialized_document_t {
        msgpack::sbuffer structure;
        msgpack::sbuffer data;
    };

    serialized_document_t serialize(const document_ptr& document);
    document_ptr deserialize(const serialized_document_t& serialized_document);
}