#pragma once
#include <memory>
#include <unordered_map>

#include <goblin-engineer/core.hpp>

#include "log/log.hpp"

#include "forward.hpp"
#include "protocol/select.hpp"
#include "protocol/insert.hpp"
#include "route.hpp"

namespace kv {

    class value_t final {
    public:
        std::string& as_string() {
            return data_;
        }

        value_t(std::string r)
            : data_(std::move(r)) {
        }

    private:
        std::string data_;
    };

    class collection_t final : public goblin_engineer::abstract_service {
    public:
        collection_t(database_ptr database, log_t& log);

        void select(session_id_t session_id, query_t& query);

        void insert(session_id_t session_id, insert_t& value);

        void erase(session_id_t session_id, query_t& query);

    private:
        log_t log_;
        std::unordered_map<composition_key_t, value_t> storage_;
    };
} // namespace kv