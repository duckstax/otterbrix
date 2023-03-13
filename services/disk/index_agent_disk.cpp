#include "index_agent_disk.hpp"
#include <components/index/disk/route.hpp>
#include "manager_disk.hpp"
#include "result.hpp"

namespace services::disk {

    index_agent_disk_t::index_agent_disk_t(base_manager_disk_t* manager,
                                           const path_t& path_db,
                                           const collection_name_t& collection_name,
                                           const index_name_t& index_name,
                                           index_disk_t::compare compare_type,
                                           log_t& log)
        : actor_zeta::basic_async_actor(manager, index_name)
        , log_(log.clone())
        , index_disk_(std::make_unique<index_disk_t>(path_db / "indexes" / collection_name / index_name, compare_type)) {
        trace(log_, "index_agent_disk::create {}", index_name);
    }

    index_agent_disk_t::~index_agent_disk_t() {
        trace(log_, "delete index_agent_disk_t");
    }

    void index_agent_disk_t::drop(session_id_t& session) {
        trace(log_, "index_agent_disk_t::drop");
        index_disk_->drop();
        actor_zeta::send(current_message()->sender(), address(), index::handler_id(index::route::success), session);
    }

    void index_agent_disk_t::insert(session_id_t& session, const wrapper_value_t& key, const document_id_t& value) {
        trace(log_, "index_agent_disk_t::insert {}", value.to_string());
        index_disk_->insert(key, value);
        actor_zeta::send(current_message()->sender(), address(), index::handler_id(index::route::success), session);
    }

    void index_agent_disk_t::remove(session_id_t& session, const wrapper_value_t& key, const document_id_t& value) {
        trace(log_, "index_agent_disk_t::remove {}", value.to_string());
        index_disk_->remove(key, value);
        actor_zeta::send(current_message()->sender(), address(), index::handler_id(index::route::success), session);
    }

    void index_agent_disk_t::find(session_id_t& session, const wrapper_value_t& value) {
        trace(log_, "index_agent_disk_t::find");
        auto result = index_disk_->find(value);
        actor_zeta::send(current_message()->sender(), address(), index::handler_id(index::route::success_find), session, result);
    }

    void index_agent_disk_t::lower_bound(session_id_t& session, const wrapper_value_t& value) {
        trace(log_, "index_agent_disk_t::lower_bound");
        auto result = index_disk_->lower_bound(value);
        actor_zeta::send(current_message()->sender(), address(), index::handler_id(index::route::success_find), session, result);
    }

    void index_agent_disk_t::upper_bound(session_id_t& session, const wrapper_value_t& value) {
        trace(log_, "index_agent_disk_t::upper_bound");
        auto result = index_disk_->upper_bound(value);
        actor_zeta::send(current_message()->sender(), address(), index::handler_id(index::route::success_find), session, result);
    }

} //namespace services::disk