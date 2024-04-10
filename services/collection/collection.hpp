#pragma once

#include <memory>
#include <unordered_map>

#include <core/btree/btree.hpp>
#include <core/pmr.hpp>

#include <components/cursor/cursor.hpp>
#include <components/document/document.hpp>
#include <components/document/document_view.hpp>
#include <components/index/index_engine.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node.hpp>
#include <components/pipeline/context.hpp>
#include <components/ql/aggregate/limit.hpp>
#include <components/ql/index.hpp>
#include <components/session/session.hpp>
#include <components/statistic/statistic.hpp>

#include <utility>

#include "forward.hpp"
#include "route.hpp"
#include "session/session.hpp"

namespace services {
    class memory_storage_t;
}

namespace services::collection {

    using document_id_t = components::document::document_id_t;
    using document_ptr = components::document::document_ptr;
    using storage_t = core::pmr::btree::btree_t<document_id_t, document_ptr>;
    using document_view_t = components::document::document_view_t;

    class collection_t;

    class context_collection_t final {
    public:
        explicit context_collection_t(std::pmr::memory_resource* resource,
                                      const collection_full_name_t& name,
                                      sessions::sessions_storage_t& sessions,
                                      actor_zeta::address_t mdisk,
                                      log_t&& log)
            : resource_(resource)
            , index_engine_(core::pmr::make_unique<components::index::index_engine_t>(resource_))
            , statistic_(resource_)
            , storage_(resource_)
            , name_(name)
            , sessions_(sessions)
            , mdisk_(mdisk)
            , log_(log) {
            assert(resource != nullptr);
        }

        storage_t& storage() noexcept { return storage_; }

        components::index::index_engine_ptr& index_engine() noexcept { return index_engine_; }

        components::statistic::statistic_t& statistic() noexcept { return statistic_; }

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        log_t& log() noexcept { return log_; }

        const collection_full_name_t& name() const noexcept { return name_; }
        sessions::sessions_storage_t& sessions() noexcept { return sessions_; }

        actor_zeta::address_t disk() noexcept { return mdisk_; }

    private:
        std::pmr::memory_resource* resource_;
        components::index::index_engine_ptr index_engine_;
        /**
        *  statistics
        */
        components::statistic::statistic_t statistic_;
        storage_t storage_;
        collection_full_name_t name_;
        /**
         * @brief Index create/drop context
         * 
         */
        sessions::sessions_storage_t& sessions_;
        actor_zeta::address_t mdisk_;
        log_t log_;
    };

    class collection_t final : public actor_zeta::basic_async_actor {
    public:
        collection_t(services::memory_storage_t* memory_storage,
                     const collection_full_name_t& name,
                     log_t& log,
                     actor_zeta::address_t mdisk);
        ~collection_t();
        auto create_documents(session_id_t& session, std::pmr::vector<document_ptr>& documents) -> void;
        auto size(session_id_t& session) -> void;

        auto execute_sub_plan(const components::session::session_id_t& session,
                              collection::operators::operator_ptr plan,
                              components::ql::storage_parameters parameters) -> void;

        void drop(const session_id_t& session);
        void close_cursor(session_id_t& session);
        void create_index_finish(const session_id_t& session,
                                 const std::string& name,
                                 const actor_zeta::address_t& index_address);
        void create_index_finish_fail(const session_id_t& session,
                                      const std::string& name,
                                      const actor_zeta::address_t& index_address);
        void index_modify_finish(const session_id_t& session);
        void index_find_finish(const session_id_t& session, const std::pmr::vector<document_id_t>& result);

        context_collection_t* view() const;

        context_collection_t* extract();

    private:
        void aggregate_document_impl(const components::session::session_id_t& session,
                                     const actor_zeta::address_t& sender,
                                     operators::operator_ptr plan);
        void update_document_impl(const components::session::session_id_t& session,
                                  const actor_zeta::address_t& sender,
                                  operators::operator_ptr plan);
        void insert_document_impl(const components::session::session_id_t& session,
                                  const actor_zeta::address_t& sender,
                                  operators::operator_ptr plan);
        void delete_document_impl(const components::session::session_id_t& session,
                                  const actor_zeta::address_t& sender,
                                  operators::operator_ptr plan);

        std::size_t size_() const;
        bool drop_();

        log_t& log() noexcept;

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