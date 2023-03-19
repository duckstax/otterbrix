#include "collection.hpp"

#include <components/index/disk/route.hpp>
#include <components/index/single_field_index.hpp>
#include <services/disk/index_disk.hpp>

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

    services::disk::index_disk_t::compare get_compare_type(const components::ql::keys_base_storage_t &keys,
                                                           const std::unique_ptr<context_collection_t> &context) {
        using compare = services::disk::index_disk_t::compare;
        if (context->storage().empty()) {
            return compare::str;
        }
        if (keys.empty()) {
            return compare::str;
        }
        if (keys.size() > 1) { //todo: other kind indexes
            return compare::str;
        }
        auto doc = document_view_t{context->storage().begin()->second};
        auto key = keys.front().as_string();
        //todo: other value types
        if (doc.is_bool(key)) {
            return compare::bool8;
        }
        if (doc.is_ulong(key)) {
            return compare::uint64;
        }
        if (doc.is_long(key)) {
            return compare::int64;
        }
        if (doc.is_double(key)) {
            return compare::float64;
        }
        return compare::str;
    }

    void collection_t::create_index(const session_id_t& session, create_index_t& index) {
        debug(log(), "collection::create_index : {} {} {}", name_, name_index_type(index.index_type_), keys_index(index.keys_)); //todo: maybe delete
        if (dropped_) {
            actor_zeta::send(current_message()->sender(), address(), handler_id(route::create_index_finish), session, result_create_index(false));
        } else {
            switch (index.index_type_) {

                case index_type::single: {
                    auto id_index = make_index<single_field_index_t>(context_->index_engine(), index.name(), index.keys_);
                    sessions::make_session(sessions_, session, sessions::create_index{current_message()->sender(), id_index});
                    actor_zeta::send(mdisk_, address(), index::handler_id(index::route::create), session, name_, index.name(), get_compare_type(index.keys_, context_));
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
    }


    void collection_t::create_index_finish(const session_id_t& session, const actor_zeta::address_t& index_address) {
        auto &create_index = sessions::find(sessions_, session).get<sessions::create_index>();
        components::index::set_disk_agent(context_->index_engine(), create_index.id_index, index_address);
        insert(context_->index_engine(), create_index.id_index, context_->storage());
        actor_zeta::send(create_index.client, address(), handler_id(route::create_index_finish), session, result_create_index(true));
        sessions::remove(sessions_, session);
    }

} // namespace services::collection