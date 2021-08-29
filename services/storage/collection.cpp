#include "collection.hpp"
#include "database.hpp"

#include "protocol/insert.hpp"
#include "protocol/request_select.hpp"

namespace services::storage {
    collection_t::collection_t(database_ptr database, log_t& log)
        : goblin_engineer::abstract_service(database, "collection")
        , log_(log.clone()) {
        add_handler(collection::select, &collection_t::select);
        add_handler(collection::insert, &collection_t::insert);
        add_handler(collection::erase, &collection_t::erase);
    }
    void collection_t::select(const session_t& session_id, const select_t& query) {
        log_.debug("start void collection_t::select");
        auto it = storage_.find(query.keys_[0]);
        log_.debug("stop void collection_t::select");
    }
    void collection_t::insert(const session_t& session_id, const  insert_t& value) {
        log_.debug("start void collection_t::insert");
        storage_.emplace(value.column_name_[0], value_t(std::move(value.values_[0])));
        log_.debug("stop void collection_t::insert");
    }
    void collection_t::erase(const session_t& session_id, const  erase_t& query) {
        storage_.erase(query.uid_);
    }

}