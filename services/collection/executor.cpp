#include "executor.hpp"

#include <components/index/disk/route.hpp>
#include <components/physical_plan/collection/operators/operator_delete.hpp>
#include <components/physical_plan/collection/operators/operator_insert.hpp>
#include <components/physical_plan/collection/operators/operator_update.hpp>
#include <components/physical_plan/collection/operators/scan/primary_key_scan.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <core/system_command.hpp>
#include <services/disk/route.hpp>
#include <services/memory_storage/memory_storage.hpp>

using namespace components::cursor;

namespace services::collection::executor {

    plan_t::plan_t(std::stack<collection::operators::operator_ptr>&& sub_plans,
                   components::ql::storage_parameters parameters,
                   services::context_storage_t&& context_storage)
        : sub_plans(std::move(sub_plans))
        , parameters(parameters)
        , context_storage_(context_storage) {}

    executor_t::executor_t(services::memory_storage_t* memory_storage, log_t&& log)
        : actor_zeta::basic_actor<executor_t>{memory_storage}
        , memory_storage_(memory_storage->address())
        , plans_(resource())
        , log_(log)
        , execute_plan_(
              actor_zeta::make_behavior(resource(), handler_id(route::execute_plan), this, &executor_t::execute_plan))
        , create_documents_(actor_zeta::make_behavior(resource(),
                                                      handler_id(route::create_documents),
                                                      this,
                                                      &executor_t::create_documents))
        , create_index_finish_(actor_zeta::make_behavior(resource(),
                                                         handler_id(index::route::success_create),
                                                         this,
                                                         &executor_t::create_index_finish))
        , create_index_finish_index_exist_(actor_zeta::make_behavior(resource(),
                                                                     handler_id(index::route::error),
                                                                     this,
                                                                     &executor_t::create_index_finish_index_exist))
        , index_modify_finish_(actor_zeta::make_behavior(resource(),
                                                         handler_id(index::route::success),
                                                         this,
                                                         &executor_t::index_modify_finish))
        , index_find_finish_(actor_zeta::make_behavior(resource(),
                                                       handler_id(index::route::success_find),
                                                       this,
                                                       &executor_t::index_find_finish)) {}

    actor_zeta::behavior_t executor_t::behavior() {
        return actor_zeta::make_behavior(resource(), [this](actor_zeta::message* msg) -> void {
            switch (msg->command()) {
                case handler_id(route::execute_plan): {
                    execute_plan_(msg);
                    break;
                }
                case handler_id(route::create_documents): {
                    create_documents_(msg);
                    break;
                }
                case handler_id(index::route::success_create): {
                    create_index_finish_(msg);
                    break;
                }
                case handler_id(index::route::error): {
                    create_index_finish_index_exist_(msg);
                    break;
                }
                case handler_id(index::route::success): {
                    index_modify_finish_(msg);
                    break;
                }
                case handler_id(index::route::success_find): {
                    index_find_finish_(msg);
                    break;
                }
            }
        });
    }

    auto executor_t::make_type() const noexcept -> const char* const { return "executor"; }

    void executor_t::execute_plan(const components::session::session_id_t& session,
                                  components::logical_plan::node_ptr logical_plan,
                                  components::ql::storage_parameters parameters,
                                  services::context_storage_t&& context_storage) {
        trace(log_, "executor::execute_plan, session: {}", session.data());
        auto plan = collection::planner::create_plan(context_storage, logical_plan, components::ql::limit_t::unlimit());
        if (!plan) {
            actor_zeta::send(memory_storage_,
                             address(),
                             handler_id(route::execute_plan_finish),
                             session,
                             make_cursor(resource(), error_code_t::create_phisical_plan_error, "invalid query plan"));
            return;
        }
        plan->set_as_root();
        traverse_plan_(session, std::move(plan), std::move(parameters), std::move(context_storage));
    }

    void executor_t::create_documents(const components::session::session_id_t& session,
                                      context_collection_t* collection,
                                      const std::pmr::vector<document_ptr>& documents) {
        trace(log_,
              "executor_t::create_documents: {}::{}, count: {}",
              collection->name().database,
              collection->name().collection,
              documents.size());
        //components::pipeline::context_t pipeline_context{session, address(), components::ql::storage_parameters{}};
        //insert_(&pipeline_context, documents);
        for (const auto& doc : documents) {
            collection->storage().emplace(components::document::get_document_id(doc), doc);
        }
        actor_zeta::send(current_message()->sender(), address(), handler_id(route::create_documents_finish), session);
    }

    void executor_t::traverse_plan_(const components::session::session_id_t& session,
                                    collection::operators::operator_ptr&& plan,
                                    components::ql::storage_parameters&& parameters,
                                    services::context_storage_t&& context_storage) {
        std::stack<collection::operators::operator_ptr> look_up;
        std::stack<collection::operators::operator_ptr> sub_plans;
        look_up.push(plan);
        while (!look_up.empty()) {
            auto check_op = look_up.top();
            while (check_op->right() == nullptr) {
                check_op = check_op->left();
                if (check_op == nullptr) {
                    break;
                }
            }
            sub_plans.push(look_up.top());
            look_up.pop();
            if (check_op != nullptr) {
                look_up.push(check_op->right());
                look_up.push(check_op->left());
            }
        }

        trace(log_, "executor::subplans count {}", sub_plans.size());
        // start execution chain by sending first avaliable sub_plan
        auto current_plan =
            (plans_.emplace(session, plan_t{std::move(sub_plans), parameters, std::move(context_storage)}))
                .first->second.sub_plans.top();
        execute_sub_plan_(session, current_plan, parameters);
    }

    void executor_t::execute_sub_plan_(const components::session::session_id_t& session,
                                       collection::operators::operator_ptr plan,
                                       components::ql::storage_parameters parameters) {
        trace(log_, "executor::execute_sub_plan, session: {}", session.data());
        if (!plan) {
            execute_sub_plan_finish_(
                session,
                make_cursor(resource(), error_code_t::create_phisical_plan_error, "invalid query plan"));
            return;
        }
        auto collection = plan->context();
        if (collection && collection->dropped()) {
            execute_sub_plan_finish_(session,
                                     make_cursor(resource(), error_code_t::collection_dropped, "collection dropped"));
            return;
        }
        components::pipeline::context_t pipeline_context{session, address(), memory_storage_, parameters};
        plan->on_execute(&pipeline_context);
        if (!plan->is_executed()) {
            sessions::make_session(
                collection->sessions(),
                session,
                sessions::suspend_plan_t{memory_storage_, std::move(plan), std::move(pipeline_context)});
            return;
        }

        switch (plan->type()) {
            case operators::operator_type::insert: {
                insert_document_impl(session, collection, std::move(plan));
                return;
            }
            case operators::operator_type::remove: {
                delete_document_impl(session, collection, std::move(plan));
                return;
            }
            case operators::operator_type::update: {
                update_document_impl(session, collection, std::move(plan));
                return;
            }
            case operators::operator_type::join:
            case operators::operator_type::aggregate: {
                aggregate_document_impl(session, collection, std::move(plan));
                return;
            }
            case operators::operator_type::add_index:
            case operators::operator_type::drop_index: {
                // nothing to do
                return;
            }
            default: {
                execute_sub_plan_finish_(session, make_cursor(resource(), operation_status_t::success));
                return;
            }
        }
    }

    void executor_t::execute_sub_plan_finish_(const components::session::session_id_t& session, cursor_t_ptr result) {
        if (result->is_error() || !plans_.contains(session)) {
            execute_plan_finish_(session, std::move(result));
        }
        auto& plan = plans_.at(session);
        if (plan.sub_plans.size() == 1) {
            execute_plan_finish_(session, std::move(result));
        } else {
            assert(!plan.sub_plans.empty() && "executor_t:execute_sub_plan_finish_: sub plans execution failed");
            plan.sub_plans.pop();
            execute_sub_plan_(session, plan.sub_plans.top(), plan.parameters);
        }
    }

    void executor_t::execute_plan_finish_(const components::session::session_id_t& session, cursor_t_ptr&& cursor) {
        trace(log_, "executor::execute_plan_finish, success: {}", cursor->is_success());
        actor_zeta::send(memory_storage_,
                         address(),
                         handler_id(route::execute_plan_finish),
                         session,
                         std::move(cursor));
        plans_.erase(session);
    }

    void executor_t::aggregate_document_impl(const components::session::session_id_t& session,
                                             context_collection_t* collection,
                                             operators::operator_ptr plan) {
        if (plan->type() == operators::operator_type::aggregate) {
            trace(log_, "executor::execute_plan : operators::operator_type::agreggate");
        } else {
            trace(log_, "executor::execute_plan : operators::operator_type::join");
        }
        if (plan->is_root()) {
            auto cursor = std::make_unique<components::cursor::sub_cursor_t>(resource(),
                                                                             collection ? collection->name()
                                                                                        : collection_full_name_t{});
            if (plan->output()) {
                for (const auto& document : plan->output()->documents()) {
                    cursor->append(document);
                }
            }
            if (cursor.get()->size() == 0) {
                execute_sub_plan_finish_(session, make_cursor(resource(), operation_status_t::failure));
            } else {
                auto result = make_cursor(resource(), operation_status_t::success);
                result->push(std::move(cursor));
                execute_sub_plan_finish_(session, std::move(result));
            }
        } else {
            execute_sub_plan_finish_(session, make_cursor(resource(), operation_status_t::success));
        }
    }

    void executor_t::update_document_impl(const components::session::session_id_t& session,
                                          context_collection_t* collection,
                                          operators::operator_ptr plan) {
        trace(log_, "executor::execute_plan : operators::operator_type::update");

        if (plan->output()) {
            auto new_id = components::document::get_document_id(plan->output()->documents().front());
            std::pmr::vector<document_id_t> documents{resource()};
            documents.emplace_back(new_id);
            actor_zeta::send(collection->disk(),
                             address(),
                             disk::handler_id(disk::route::remove_documents),
                             session,
                             collection->name().database,
                             collection->name().collection,
                             documents);
            auto cursor(new cursor_t(resource()));
            auto sub_cursor = std::make_unique<sub_cursor_t>(resource(), collection->name());
            for (const auto& id : documents) {
                sub_cursor->append(collection->storage().at(id));
            }
            cursor->push(std::move(sub_cursor));
            execute_sub_plan_finish_(session, cursor);
        } else {
            if (plan->modified()) {
                auto cursor(new cursor_t(resource()));
                auto sub_cursor = std::make_unique<sub_cursor_t>(resource(), collection->name());
                for (const auto& id : plan->modified()->documents()) {
                    sub_cursor->append(collection->storage().at(id));
                }
                cursor->push(std::move(sub_cursor));
                actor_zeta::send(collection->disk(),
                                 address(),
                                 disk::handler_id(disk::route::remove_documents),
                                 session,
                                 collection->name().database,
                                 collection->name().collection,
                                 plan->modified()->documents());
                execute_sub_plan_finish_(session, cursor);
            } else {
                auto cursor(new cursor_t(resource()));
                cursor->push(std::make_unique<sub_cursor_t>(resource(), collection->name()));
                actor_zeta::send(collection->disk(),
                                 address(),
                                 disk::handler_id(disk::route::remove_documents),
                                 session,
                                 collection->name().database,
                                 collection->name().collection,
                                 std::pmr::vector<document_id_t>{resource()});
                execute_sub_plan_finish_(session, cursor);
            }
        }
    }

    void executor_t::insert_document_impl(const components::session::session_id_t& session,
                                          context_collection_t* collection,
                                          operators::operator_ptr plan) {
        trace(log_,
              "executor::execute_plan : operators::operator_type::insert {}",
              plan->output() ? plan->output()->documents().size() : 0);
        actor_zeta::send(collection->disk(),
                         address(),
                         disk::handler_id(disk::route::write_documents),
                         session,
                         collection->name().database,
                         collection->name().collection,
                         plan->output() ? std::move(plan->output()->documents())
                                        : std::pmr::vector<document_ptr>{resource()});

        auto cursor = make_cursor(resource());
        auto sub_cursor = std::make_unique<sub_cursor_t>(resource(), collection->name());
        if (plan->modified()) {
            for (const auto& id : plan->modified()->documents()) {
                sub_cursor->append(collection->storage().at(id));
            }
        } else {
            for (const auto& doc : collection->storage()) {
                sub_cursor->append(doc.second);
            }
        }
        cursor->push(std::move(sub_cursor));
        execute_sub_plan_finish_(session, cursor);
    }

    void executor_t::delete_document_impl(const components::session::session_id_t& session,
                                          context_collection_t* collection,
                                          operators::operator_ptr plan) {
        trace(log_, "executor::execute_plan : operators::operator_type::remove");

        auto modified = plan->modified() ? plan->modified()->documents() : std::pmr::vector<document_id_t>{resource()};
        actor_zeta::send(collection->disk(),
                         address(),
                         disk::handler_id(disk::route::remove_documents),
                         session,
                         collection->name().database,
                         collection->name().collection,
                         modified);
        auto sub_cursor = std::make_unique<sub_cursor_t>(resource(), collection->name());
        for (const auto& _ : plan->modified()->documents()) {
            sub_cursor->append(nullptr);
        }
        auto cursor = make_cursor(resource());
        cursor->push(std::move(sub_cursor));
        execute_sub_plan_finish_(session, cursor);
    }

} // namespace services::collection::executor