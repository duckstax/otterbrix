#include "collection.hpp"

#include <components/index/disk/route.hpp>
#include <components/index/single_field_index.hpp>
#include <components/ql/index.hpp>
#include <services/disk/index_disk.hpp>

#include <services/collection/planner/create_plan.hpp>
#include <services/disk/route.hpp>
#include <services/memory_storage/memory_storage.hpp>

using components::ql::create_index_t;
using components::ql::index_type;

using components::index::make_index;
using components::index::single_field_index_t;

using namespace components::cursor;
using namespace core::pmr;

namespace services::collection {

    std::string keys_index(const components::ql::keys_base_storage_t& keys) {
        std::string result;
        for (const auto& key : keys) {
            if (!result.empty()) {
                result += ",";
            }
            result += key.as_string();
        }
        return result;
    }

    void collection_t::create_index_finish(const session_id_t& session,
                                           const std::string& name,
                                           const actor_zeta::address_t& index_address) {
        debug(log(), "collection::create_index_finish");
        auto& create_index = sessions::find(sessions_, session, name).get<sessions::create_index_t>();
        components::index::set_disk_agent(context_->index_engine(), create_index.id_index, index_address);
        components::index::insert(context_->index_engine(), create_index.id_index, context_->storage());
        actor_zeta::send(create_index.client,
                         address(),
                         handler_id(route::execute_plan_finish),
                         session,
                         make_cursor(default_resource(), operation_status_t::success));
        sessions::remove(sessions_, session, name);
    }

    void collection_t::index_modify_finish(const session_id_t& session) {
        debug(log(), "collection::index_modify_finish");
        sessions::remove(sessions_, session);
    }

    void collection_t::index_find_finish(const session_id_t& session, const std::pmr::vector<document_id_t>& result) {
        debug(log(), "collection::index_find_result: {}", result.size());
        components::index::sync_index_from_disk(context_->index_engine(),
                                                current_message()->sender(),
                                                result,
                                                context_->storage());
        auto& suspend_plan = sessions::find(sessions_, session).get<sessions::suspend_plan_t>();
        auto res = cursor_storage_.emplace(
            session,
            std::make_unique<components::cursor::sub_cursor_t>(context_->resource(), address()));
        suspend_plan.plan->on_execute(&suspend_plan.pipeline_context);
        if (suspend_plan.plan->is_executed()) {
            if (suspend_plan.plan->output()) {
                for (const auto& document : suspend_plan.plan->output()->documents()) {
                    res.first->second->append(document_view_t(document));
                }
            }
        }
        sessions::remove(sessions_, session);
        auto cursor = make_cursor(default_resource());
        cursor->push(res.first->second.get());
        actor_zeta::send(suspend_plan.client, address(), handler_id(route::execute_sub_plan_finish), session, cursor);
    }

} // namespace services::collection
