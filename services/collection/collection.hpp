#pragma once

#include <memory>
#include <unordered_map>

#include <core/btree/btree.hpp>
#include <core/pmr.hpp>

#include <components/cursor/cursor.hpp>
#include <components/document/document.hpp>
#include <components/index/index_engine.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node.hpp>
#include <components/physical_plan/context.hpp>
#include <components/ql/aggregate/limit.hpp>
#include <components/ql/index.hpp>
#include <components/session/session.hpp>

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
    using cursor_storage_t = std::pmr::unordered_map<session_id_t, std::unique_ptr<components::cursor::sub_cursor_t>>;

    namespace executor {
        class executor_t;
    }

    struct pending_index_create {
        components::ql::create_index_ptr index_ql{nullptr};
        std::unique_ptr<components::pipeline::context_t> context{nullptr};
    };

    class context_collection_t final {
    public:
        explicit context_collection_t(std::pmr::memory_resource* resource,
                                      const collection_full_name_t& name,
                                      const actor_zeta::address_t& mdisk,
                                      log_t&& log)
            : resource_(resource)
            , cursor_storage_(resource_)
            , index_engine_(core::pmr::make_unique<components::index::index_engine_t>(resource_))
            , storage_(resource_)
            , name_(name)
            , mdisk_(mdisk)
            , log_(log) {
            assert(resource != nullptr);
        }

        storage_t& storage() noexcept { return storage_; }
        cursor_storage_t& cursor_storage() noexcept { return cursor_storage_; }

        components::index::index_engine_ptr& index_engine() noexcept { return index_engine_; }

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        log_t& log() noexcept { return log_; }

        const collection_full_name_t& name() const noexcept { return name_; }

        sessions::sessions_storage_t& sessions() noexcept { return sessions_; }

        bool drop() noexcept {
            if (dropped_) {
                return false;
            }
            dropped_ = true;
            return true;
        }

        bool dropped() const noexcept { return dropped_; }

        actor_zeta::address_t disk() noexcept { return mdisk_; }

        std::vector<pending_index_create>& pending_indexes() noexcept { return pending_indexes_to_create; }

    private:
        std::pmr::memory_resource* resource_;
        cursor_storage_t cursor_storage_;
        components::index::index_engine_ptr index_engine_;

        storage_t storage_;
        collection_full_name_t name_;
        /**
         * @brief Index create/drop context
         */
        sessions::sessions_storage_t sessions_;
        actor_zeta::address_t mdisk_;
        log_t log_;

        std::vector<pending_index_create> pending_indexes_to_create;
        bool dropped_{false};
    };

} // namespace services::collection