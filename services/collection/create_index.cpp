#include "collection.hpp"

#include "components/index/single_field_index.hpp"

using components::ql::create_index_t;
using components::ql::index_type;

using components::index::make_index;
using components::index::single_field_index_t;

namespace services::collection {

    std::string name_index_type(index_type type) {
        switch (type) {
            case index_type::single:
                return "single";
            case index_type::composite:
                return "composite";
            case index_type::multikey:
                return "multikey";
            case index_type::hashed:
                return "hashed";
            case index_type::wildcard:
                return "wildcard";
        }
        return "default";
    }

    std::string keys_index(const components::ql::keys_base_storage_t &keys) {
        std::string result;
        for (const auto &key : keys) {
            if (!result.empty()) {
                result += ",";
            }
            result += key.as_string();
        }
        return result;
    }

    void collection_t::create_index(const session_id_t& session, create_index_t& index) {
        debug(log(), "collection::create_index : {} {} {}", name_, name_index_type(index.index_type_), keys_index(index.keys_)); //todo: maybe delete
        if (dropped_) {
            actor_zeta::send(current_message()->sender(), address(), handler_id(route::create_index_finish), session, result_create_index(false));
        } else {
            switch (index.index_type_) {

                case index_type::single: {
                    auto id_index = make_index<single_field_index_t>(context_->index_engine(), index.name(), index.keys_);
                    insert(context_->index_engine(), id_index, context_->storage());
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
            //todo: send into disk
            actor_zeta::send(current_message()->sender(), address(), handler_id(route::create_index_finish), session, result_create_index(true));
        }
    }

} // namespace services::collection