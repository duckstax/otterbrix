#pragma once

#include <memory>
#include <unordered_map>

#include <absl/container/btree_map.h>

#include <core/btree/btree.hpp>
#include <core/pmr.hpp>

#include <components/cursor/cursor.hpp>
#include <components/document/document.hpp>
#include <components/document/document_view.hpp>
#include <components/index/index_engine.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node.hpp>
#include <components/ql/index.hpp>
#include <components/session/session.hpp>
#include <components/statistic/statistic.hpp>
#include <components/transaction/context.hpp>

#include <services/collection/operators/predicates/limit.hpp>
#include <services/database/database.hpp>
#include <utility>

#include "forward.hpp"
#include "result.hpp"
#include "route.hpp"
#include "session/session.hpp"

namespace services::collection {

    using document_id_t = components::document::document_id_t;
    using document_ptr = components::document::document_ptr;
    using storage_t = core::pmr::btree::btree_t<document_id_t, document_ptr>;
    using document_view_t = components::document::document_view_t;

    class context_collection_t final {
    public:
        explicit context_collection_t(std::pmr::memory_resource* resource, log_t&& log)
            : resource_(resource)
            , log_(log)
            , index_engine_(core::pmr::make_unique<components::index::index_engine_t>(resource_))
            , statistic_(resource_)
            , storage_(resource_) {
            assert(resource != nullptr);
        }

        storage_t& storage() noexcept {
            return storage_;
        }

        components::index::index_engine_ptr& index_engine() noexcept {
            return index_engine_;
        }

        components::statistic::statistic_t& statistic() noexcept {
            return statistic_;
        }

        std::pmr::memory_resource* resource() const noexcept {
            return resource_;
        }

        log_t& log() noexcept {
            return log_;
        }

    private:
        std::pmr::memory_resource* resource_;
        log_t log_;
        /**
        *  index
        */

        components::index::index_engine_ptr index_engine_;
        /**
        *  statistics
        */
        components::statistic::statistic_t statistic_;
        storage_t storage_;
    };

    class collection_t final : public actor_zeta::basic_async_actor {
    public:
        collection_t(database::database_t*, const std::string& name, log_t& log, actor_zeta::address_t mdisk);
        ~collection_t();
        auto create_documents(session_id_t& session, std::pmr::vector<document_ptr>& documents) -> void;
        auto size(session_id_t& session) -> void;
        void insert_one(session_id_t& session_t, document_ptr& document);
        void insert_many(session_id_t& session, std::pmr::vector<document_ptr>& documents);

        auto find(
                const components::session::session_id_t& session,
                const components::logical_plan::node_ptr& logic_plan,
                components::ql::storage_parameters parameters) -> void;

        auto find_one(
                const components::session::session_id_t& session,
                const components::logical_plan::node_ptr& logic_plan,
                components::ql::storage_parameters parameters) -> void;

        auto delete_one(
                const components::session::session_id_t& session,
                const components::logical_plan::node_ptr& logic_plan,
                components::ql::storage_parameters parameters) -> void;

        auto delete_many(
                const components::session::session_id_t& session,
                const components::logical_plan::node_ptr& logic_plan,
                components::ql::storage_parameters parameters) -> void;

        auto update_one(
                const components::session::session_id_t& session,
                const components::logical_plan::node_ptr& logic_plan,
                components::ql::storage_parameters parameters,
                const document_ptr& update,
                bool upsert) -> void;

        auto update_many(
                const components::session::session_id_t& session,
                const components::logical_plan::node_ptr& logic_plan,
                components::ql::storage_parameters parameters,
                const document_ptr& update,
                bool upsert) -> void;

        void drop(const session_id_t& session);
        void close_cursor(session_id_t& session);

        void create_index(const session_id_t& session, components::ql::create_index_t& index);
        void create_index_finish(const session_id_t& session, const actor_zeta::address_t& index_address);

        context_collection_t* view() const;

        context_collection_t* extract();

    private:
        std::pmr::vector<document_id_t> insert_(components::transaction::context_t* transaction_context, const std::pmr::vector<document_ptr>& documents);
        std::size_t size_() const;
        bool drop_();
        void delete_(const session_id_t& session, const components::logical_plan::node_ptr& logic_plan, components::ql::storage_parameters parameters, const operators::predicates::limit_t &limit);
        void update_(const session_id_t& session, const components::logical_plan::node_ptr& logic_plan, components::ql::storage_parameters parameters, const document_ptr& update, bool upsert, const operators::predicates::limit_t &limit);
        void send_update_to_disk_(const session_id_t& session, const result_update& result);
        void send_delete_to_disk_(const session_id_t& session, const result_delete& result);

        log_t& log() noexcept;

        const std::string name_;
        const std::string database_name_;
        actor_zeta::address_t database_;
        actor_zeta::address_t mdisk_;

        std::unique_ptr<context_collection_t> context_;
        std::pmr::unordered_map<session_id_t, std::unique_ptr<components::cursor::sub_cursor_t>> cursor_storage_;
        sessions::sessions_storage_t sessions_;
        bool dropped_{false};

#ifdef DEV_MODE
    public:
        std::size_t size_test() const;
#endif
    };

} // namespace services::collection
