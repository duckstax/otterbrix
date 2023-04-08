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
        add_handler(handler_id(index::route::insert), &index_agent_disk_t::insert);
        add_handler(handler_id(index::route::remove), &index_agent_disk_t::remove);
        add_handler(handler_id(index::route::find), &index_agent_disk_t::find);
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

    void index_agent_disk_t::find(session_id_t& session, wrapper_value_t value, components::expressions::compare_type compare) {
        using components::expressions::compare_type;

        trace(log_, "index_agent_disk_t::find");
        debug(log_, "FIND: {}", compare);
        debug(log_, "FIND: {}", value->as_int());
        index_disk_t::result res;
        switch (compare) {
            case compare_type::eq:
                index_disk_->find(value, res);
                break;
            case compare_type::ne:
                index_disk_->lower_bound(value, res);
                index_disk_->upper_bound(value, res);
                break;
            case compare_type::gt:
                index_disk_->upper_bound(value, res);
                break;
            case compare_type::lt:
                index_disk_->lower_bound(value, res);
                break;
            case compare_type::gte:
                index_disk_->find(value, res);
                index_disk_->upper_bound(value, res);
                break;
            case compare_type::lte:
                index_disk_->lower_bound(value, res);
                index_disk_->find(value, res);
                break;
            default:
                break;
        }
        actor_zeta::send(current_message()->sender(), address(), index::handler_id(index::route::success_find), session, res);
    }

} //namespace services::disk