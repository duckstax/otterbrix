#pragma once
#include <memory>
#include <unordered_map>

#include <goblin-engineer/core.hpp>

#include "log/log.hpp"

#include "forward.hpp"
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

    using composition_key_t = std::string;
    using query_t = std::string;

    struct insert_value_t final {
        std::string name_table_;
        std::vector<std::string> column_name_;
        std::vector<std::string> values_;
    };

    class manager_database_t final : public goblin_engineer::abstract_manager_service {
    public:
        manager_database_t(log_t& log, size_t num_workers, size_t max_throughput);

        auto executor() noexcept -> goblin_engineer::abstract_executor* final override;
        auto get_executor() noexcept -> goblin_engineer::abstract_executor* final override;
        auto enqueue_base(goblin_engineer::message_ptr msg, actor_zeta::execution_device*) -> void override;
        ~manager_database_t();
        void create() {
        }

    private:
        log_t log_;
        goblin_engineer::executor_ptr e_;
    };

    using manager_database_ptr = goblin_engineer::intrusive_ptr<manager_database_t>;

    class database_t final : public goblin_engineer::abstract_manager_service {
    public:
        database_t(manager_database_ptr supervisor, log_t& log, size_t num_workers, size_t max_throughput);
        auto executor() noexcept -> goblin_engineer::abstract_executor* final override;
        auto get_executor() noexcept -> goblin_engineer::abstract_executor* final override;
        auto enqueue_base(goblin_engineer::message_ptr msg, actor_zeta::execution_device*) -> void override;
        ~database_t();
        void collection() {
        }

    private:
        log_t log_;
        goblin_engineer::executor_ptr e_;
    };

    using database_ptr = goblin_engineer::intrusive_ptr<database_t>;

    class collection_t final : public goblin_engineer::abstract_service {
    public:
        collection_t(database_ptr database, log_t& log)
            : goblin_engineer::abstract_service(database, "collection")
            , log_(log.clone()) {
            add_handler(collection::select, &collection_t::select);
            add_handler(collection::insert, &collection_t::insert);
            add_handler(collection::erase, &collection_t::erase);
        }

        void select(session_id_t session_id, query_t& query) {
            auto it = storage_.find(query);
        }

        void insert(session_id_t session_id, insert_value_t& value) {
            storage_.emplace(value.column_name_[0], value_t(std::move(value.values_[0])));
        }

        void erase(session_id_t session_id, query_t& query) {
            storage_.erase(query);
        }

    private:
        log_t log_;
        std::unordered_map<composition_key_t, value_t> storage_;
    };
} // namespace kv