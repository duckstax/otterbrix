#include "collection.hpp"

#include "components/index/single_field_index.hpp"

using components::ql::create_index_t;
using components::ql::index_type;

using components::index::make_index;
using components::index::single_field_index_t;

namespace services::collection {

    void collection_t::create_index(create_index_t& index) {
        switch (index.index_type_) {

            case index_type::single: {
                make_index<single_field_index_t>(index_engine_, index.keys_);
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

            case index_type::default_:
            default: {
                break;
            }
        }
    }

} // namespace services::collection