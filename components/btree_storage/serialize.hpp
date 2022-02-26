#pragma once
#include "btree_storage.hpp"

namespace components::btree {

    struct serialized_document_t {
        msgpack::sbuffer structure;
        msgpack::sbuffer data;
    };

    serialized_document_t serialize(const document_unique_ptr& document);
    document_unique_ptr deserialize(const serialized_document_t& serialized_document);

}