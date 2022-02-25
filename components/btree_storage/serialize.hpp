#pragma once
#include "btree_storage.hpp"

namespace components::btree {

    msgpack::sbuffer serialize(const document_unique_ptr& document);
    document_unique_ptr deserialize(const msgpack::sbuffer& buffer);

}