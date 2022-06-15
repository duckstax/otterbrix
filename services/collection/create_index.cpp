#include "collection.hpp"

namespace services::collection {

    void collection_t::create_index() {
        make_index<single_field_index_t>(index_engine_,1);
    }

} // namespace services::collection