#pragma once

#include <filesystem>
#include <core/btree/btree.hpp>
#include <core/excutor.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/log/log.hpp>
#include <components/ql/base.hpp>
#include <components/session/session.hpp>
#include "index_disk.hpp"

namespace services::disk {
    class manager_disk_t;
    using index_name_t = std::string;

    struct base_manager_disk_t;

    class index_agent_disk_t final : public actor_zeta::basic_actor<index_agent_disk_t> {

        using path_t = std::filesystem::path;
        using session_id_t = ::components::session::session_id_t;
        using document_id_t = components::document::document_id_t;
        using wrapper_value_t = document::wrapper_value_t;

    public:
        index_agent_disk_t(manager_disk_t*,
                           const path_t&, const collection_name_t&, const index_name_t&, components::ql::index_compare compare_type, log_t&);

        ~index_agent_disk_t() final;

        const collection_name_t& collection_name() const;

        void drop(session_id_t& session);
        bool is_dropped() const;

        void insert(session_id_t& session, const wrapper_value_t& key, const document_id_t& value);
        void remove(session_id_t& session, const wrapper_value_t& key, const document_id_t& value);
        void find(session_id_t& session, const wrapper_value_t& value, components::expressions::compare_type compare);

        auto make_type() const noexcept -> const char* const;
        actor_zeta::behavior_t behavior();
    private:
        actor_zeta::behavior_t insert_;
        actor_zeta::behavior_t remove_;
        actor_zeta::behavior_t find_;
        actor_zeta::behavior_t drop_;
        log_t log_;
        std::unique_ptr<index_disk_t> index_disk_;
        const collection_name_t& collection_name_;
        bool is_dropped_ {false};
    };

    using index_agent_disk_ptr = std::unique_ptr<index_agent_disk_t>;
    using index_agent_disk_storage_t = core::pmr::btree::btree_t<index_name_t, index_agent_disk_ptr>;

} //namespace services::disk
