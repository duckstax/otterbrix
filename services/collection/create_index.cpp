#include "collection.hpp"

namespace services::collection {

    void collection_t::create_index(create_index_t& index) {
        switch (index.index_type_) {
            case index_type::default_: {
                break;
            }
            case index_type::single: {
                make_index<single_field_index_t>(index_engine_, 1);
                break;
            }
            case index_type::composite: {
                break;
            }
            case index_type::multikey: {
                break;
            }
            case index_type::hashed: {
                break;
            }
            case index_type::wildcard: {
                break;
            }
        }
    }

} // namespace services::collection