#include "executor.hpp"

#include <components/index/disk/route.hpp>
#include <components/index/single_field_index.hpp>
#include <services/disk/index_disk.hpp>

#include <components/physical_plan_generator/create_plan.hpp>
#include <services/disk/route.hpp>
#include <services/memory_storage/memory_storage.hpp>

using components::logical_plan::index_type;

using components::index::make_index;
using components::index::single_field_index_t;

using namespace components::cursor;
using namespace core::pmr;

namespace services::collection::executor {

    std::string keys_index(const components::logical_plan::keys_base_storage_t& keys) {
        std::string result;
        for (const auto& key : keys) {
            if (!result.empty()) {
                result += ",";
            }
            result += key.as_string();
        }
        return result;
    }

    void executor_t::create_index_finish(const session_id_t& session,
                                         const std::string& name,
                                         const actor_zeta::address_t& index_address,
                                         context_collection_t* collection) {
        debug(log_, "collection::create_index_finish");
        auto& create_index = sessions::find(collection->sessions(), session, name).get<sessions::create_index_t>();
        components::index::set_disk_agent(collection->index_engine(), create_index.id_index, index_address);
        components::index::insert(collection->index_engine(), create_index.id_index, collection->storage());
        // TODO: revisit filling index_disk
        if (index_address != actor_zeta::address_t::empty_address()) {
            auto* index = components::index::search_index(collection->index_engine(), create_index.id_index);
            auto range = index->keys();
            std::vector<std::pair<components::document::value_t, document_id_t>> values;
            values.reserve(collection->storage().size());
            for (auto it = range.first; it != range.second; ++it) {
                const auto& key_tmp = *it;
                const std::string& key = key_tmp.as_string(); // hack
                for (const auto& doc : collection->storage()) {
                    values.emplace_back(doc.second->get_value(key), doc.first);
                }
            }
            actor_zeta::send(index_address, address(), handler_id(index::route::insert_many), session, values);
        }
        actor_zeta::send(create_index.client,
                         address(),
                         handler_id(route::execute_plan_finish),
                         session,
                         make_cursor(resource(), operation_status_t::success));
    }

    void executor_t::create_index_finish_index_exist(const session_id_t& session,
                                                     const std::string& name,
                                                     context_collection_t* collection) {
        debug(log_, "collection::create_index_finish_index_exist");
        auto& create_index = sessions::find(collection->sessions(), session, name).get<sessions::create_index_t>();
        actor_zeta::send(
            create_index.client,
            address(),
            handler_id(route::execute_plan_finish),
            session,
            make_cursor(resource(), error_code_t::index_create_fail, "index with name : " + name + " exist"));
        sessions::remove(collection->sessions(), session, name);
    }

    void executor_t::index_modify_finish(const session_id_t& session, context_collection_t* collection) {
        debug(log_, "collection::index_modify_finish");
        sessions::remove(collection->sessions(), session);
    }

    void executor_t::index_find_finish(const session_id_t& session,
                                       const std::pmr::vector<document_id_t>& result,
                                       context_collection_t* collection) {
        debug(log_, "collection::index_find_result: {}", result.size());
        components::index::sync_index_from_disk(collection->index_engine(),
                                                current_message()->sender(),
                                                result,
                                                collection->storage());
        auto& suspend_plan = sessions::find(collection->sessions(), session).get<sessions::suspend_plan_t>();
        auto res = std::make_unique<components::cursor::sub_cursor_t>(resource(), collection->name());
        suspend_plan.plan->on_execute(&suspend_plan.pipeline_context);
        if (suspend_plan.plan->is_executed()) {
            if (suspend_plan.plan->output()) {
                for (const auto& document : suspend_plan.plan->output()->documents()) {
                    res->append(document);
                }
            }
        }
        sessions::remove(collection->sessions(), session);
        auto cursor = make_cursor(resource());
        cursor->push(std::move(res));
        execute_sub_plan_finish_(session, cursor);
    }

} // namespace services::collection::executor
