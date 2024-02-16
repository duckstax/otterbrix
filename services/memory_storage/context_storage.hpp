#pragma once

#include <components/ql/base.hpp>
#include <unordered_map>

namespace services {

    using context_storage_t =
        std::unordered_map<collection_full_name_t, collection::context_collection_t*, collection_name_hash>;

} //namespace services