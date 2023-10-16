#include "collection.hpp"
#include <components/index/disk/route.hpp>
#include <core/system_command.hpp>
#include <services/collection/operators/operator_delete.hpp>
#include <services/collection/operators/operator_insert.hpp>
#include <services/collection/operators/operator_update.hpp>
#include <services/collection/operators/scan/primary_key_scan.hpp>
#include <services/collection/planner/create_plan.hpp>
#include <services/disk/route.hpp>
#include <services/memory_storage/memory_storage.hpp>

using namespace components::result;

namespace services::collection {

    collection_t::collection_t(services::memory_storage_t* memory_storage, const collection_full_name_t& name,
                               log_t& log, actor_zeta::address_t mdisk)
        : actor_zeta::basic_actor<collection_t>(memory_storage)
        , name_(name)
        , mdisk_(std::move(mdisk))
        , context_(std::make_unique<context_collection_t>(new std::pmr::monotonic_buffer_resource(), log.clone()))
        , cursor_storage_(context_->resource())
        , create_documents_(actor_zeta::make_behavior(resource(),handler_id(route::create_documents),this, &collection_t::create_documents))
        , insert_documents_(actor_zeta::make_behavior(resource(),handler_id(route::insert_documents),this, &collection_t::insert_documents))
        , delete_documents_(actor_zeta::make_behavior(resource(),handler_id(route::delete_documents),this, &collection_t::delete_documents))
        , update_documents_(actor_zeta::make_behavior(resource(),handler_id(route::update_documents),this, &collection_t::update_documents))
        , execute_plan_(actor_zeta::make_behavior(resource(),handler_id(route::execute_plan),this, &collection_t::execute_plan))
        , size_(actor_zeta::make_behavior(resource(),handler_id(route::size),this, &collection_t::size))
        , drop_collection_(actor_zeta::make_behavior(resource(),handler_id(route::drop_collection),this, &collection_t::drop))
        , close_cursor_(actor_zeta::make_behavior(resource(),handler_id(route::close_cursor),this, &collection_t::close_cursor))
        , create_index_(actor_zeta::make_behavior(resource(),handler_id(route::create_index),this, &collection_t::create_index))
        , success_create_(actor_zeta::make_behavior(resource(),handler_id(index::route::success_create),this, &collection_t::create_index_finish))
        , drop_index_(actor_zeta::make_behavior(resource(),handler_id(route::drop_index),this, &collection_t::drop_index))
        , success_(actor_zeta::make_behavior(resource(),handler_id(index::route::success),this, &collection_t::index_modify_finish))
        , success_find_(actor_zeta::make_behavior(resource(),handler_id(index::route::success_find),this, &collection_t::index_find_finish))
    {

    }

    collection_t::~collection_t() { trace(log(), "delete collection_t"); }

    auto collection_t::make_type() const noexcept -> const char* const{
        return name_.to_string().c_str();
    }

    actor_zeta::behavior_t collection_t::behavior() {
        return actor_zeta::make_behavior(
            resource(),
            [this](actor_zeta::message* msg) -> void {
                switch (msg->command()) {
                    case handler_id(route::create_documents): {
                        create_documents_(msg);
                        break;
                    }
                    case handler_id(route::insert_documents): {
                        insert_documents_(msg);
                        break;
                    }
                    case handler_id(route::delete_documents): {
                        delete_documents_(msg);
                        break;
                    }
                    case handler_id(route::update_documents): {
                        update_documents_(msg);
                        break;
                    }
                    case handler_id(route::execute_plan): {
                        execute_plan_(msg);
                        break;
                    }
                    case handler_id(route::size): {
                        size_(msg);
                        break;
                    }
                    case handler_id(route::drop_collection): {
                        drop_collection_(msg);
                        break;
                    }
                    case handler_id(route::close_cursor): {
                        close_cursor_(msg);
                        break;
                    }
                    case handler_id(route::create_index): {
                        create_index_(msg);
                        break;
                    }
                    case handler_id(index::route::success_create): {
                        success_create_(msg);
                        break;
                    }
                    case handler_id(route::drop_index): {
                        drop_index_(msg);
                        break;
                    }
                    case handler_id(index::route::success): {
                        success_(msg);
                        break;
                    }
                    case handler_id(index::route::success_find): {
                        success_find_(msg);
                        break;
                    }
                }
            });
    }

    void collection_t::create_documents(components::session::session_id_t& session,
                                        std::pmr::vector<document_ptr>& documents) {
        trace(log(), "{}::{}::create_documents, count: {}", name_.database, name_.collection, documents.size());
        //components::pipeline::context_t pipeline_context{session, address(), components::ql::storage_parameters{}};
        //insert_(&pipeline_context, documents);
        for (const auto& doc : documents) {
            context_->storage().emplace(components::document::get_document_id(doc), doc);
        }
        actor_zeta::send(current_message()->sender(), address(), handler_id(route::create_documents_finish), session);
    }

    auto collection_t::size(session_id_t& session) -> void {
        trace(log(), "collection {}::size", name_.collection);
        auto dispatcher = current_message()->sender();
        auto result = dropped_ ? result_size() : result_size(size_impl_());
        actor_zeta::send(dispatcher, address(), handler_id(route::size_finish), session, result);
    }

    auto collection_t::execute_plan(const components::session::session_id_t& session,
                                    const components::logical_plan::node_ptr& logical_plan,
                                    components::ql::storage_parameters parameters) -> void {
        trace(log(), "collection::execute_plan : {}", name_.to_string());
        auto sender = current_message()->sender();
        if (dropped_) {
            actor_zeta::send(sender, address(), handler_id(route::execute_plan_finish), session,
                             make_error(error_code_t::collection_dropped, "collection dropped"));
            return;
        }
        auto plan = planner::create_plan(view(), logical_plan, components::ql::limit_t::unlimit());
        if (!plan) {
            actor_zeta::send(sender, address(), handler_id(route::execute_plan_finish), session,
                             make_error(error_code_t::create_phisical_plan_error, "invalid query plan"));
            return;
        }
        components::pipeline::context_t pipeline_context{session, address(), parameters};
        plan->on_execute(&pipeline_context);
        if (!plan->is_executed()) {
            sessions::make_session(sessions_, session,
                                   sessions::suspend_plan_t{sender, std::move(plan), std::move(pipeline_context)});
            return;
        }
        // todo: only find
        auto cursor = cursor_storage_.emplace(
            session, std::make_unique<components::cursor::sub_cursor_t>(context_->resource(), address()));
        if (plan->output()) {
            for (const auto& document : plan->output()->documents()) {
                cursor.first->second->append(document_view_t(document));
            }
        }
        auto result = new components::cursor::cursor_t(context_->resource());
        result->push(cursor.first->second.get());
        actor_zeta::send(sender, address(), handler_id(route::execute_plan_finish), session, make_result(result));
        //end: only find
    }

    auto collection_t::insert_documents(const components::session::session_id_t& session,
                                        const components::logical_plan::node_ptr& logic_plan,
                                        components::ql::storage_parameters parameters) -> void {
        trace(log(), "collection::insert : {}", name_.to_string());
        auto dispatcher = current_message()->sender();
        if (dropped_) {
            actor_zeta::send(dispatcher, address(), handler_id(route::insert_finish), session, nullptr);
        } else {
            auto plan = planner::create_plan(view(), logic_plan, components::ql::limit_t::unlimit());
            if (!plan) {
                actor_zeta::send(dispatcher, address(), handler_id(route::insert_finish), session, nullptr);
            } else {
                components::pipeline::context_t pipeline_context{session, address(), std::move(parameters)};
                plan->on_execute(&pipeline_context);
                if (plan->is_executed()) {
                    actor_zeta::send(mdisk_, address(), disk::handler_id(disk::route::write_documents), session,
                                     std::string(name_.database), std::string(name_.collection),
                                     plan->output() ? plan->output()->documents()
                                                    : std::pmr::vector<document_ptr>{context_->resource()});
                    actor_zeta::send(dispatcher, address(), handler_id(route::insert_finish), session,
                                     result_insert{plan->modified()
                                                       ? std::move(plan->modified()->documents())
                                                       : std::pmr::vector<document_id_t>{context_->resource()}});
                } else {
                    sessions::make_session(sessions_, session,
                                           sessions::suspend_plan_t{current_message()->sender(), std::move(plan),
                                                                    std::move(pipeline_context)});
                }
            }
        }
    }

    auto collection_t::delete_documents(const components::session::session_id_t& session,
                                        const components::logical_plan::node_ptr& logic_plan,
                                        components::ql::storage_parameters parameters) -> void {
        trace(log(), "collection::delete_documents : {}", name_.to_string());
        auto dispatcher = current_message()->sender();
        if (dropped_) {
            actor_zeta::send(dispatcher, address(), handler_id(route::delete_finish), session,
                             result_delete(context_->resource()));
            return;
        }
        auto plan = planner::create_plan(view(), logic_plan, components::ql::limit_t::limit_one());
        if (!plan) {
            actor_zeta::send(dispatcher, address(), handler_id(route::delete_finish), session,
                             result_delete(context_->resource()));
            return;
        }
        components::pipeline::context_t pipeline_context{session, address(), std::move(parameters)};
        plan->on_execute(&pipeline_context);
        if (!plan->is_executed()) {
            sessions::make_session(
                sessions_, session,
                sessions::suspend_plan_t{current_message()->sender(), std::move(plan), std::move(pipeline_context)});
            return;
        }
        auto modified =
            plan->modified() ? plan->modified()->documents() : std::pmr::vector<document_id_t>{context_->resource()};
        actor_zeta::send(mdisk_, address(), disk::handler_id(disk::route::remove_documents), session,
                         std::string(name_.database), std::string(name_.collection), modified);
        actor_zeta::send(dispatcher, address(), handler_id(route::delete_finish), session,
                         result_delete{std::move(modified)});
    }

    auto collection_t::update_documents(const components::session::session_id_t& session,
                                        const components::logical_plan::node_ptr& logic_plan,
                                        components::ql::storage_parameters parameters) -> void {
        trace(log(), "collection::update_documents : {}", name_.to_string());
        auto dispatcher = current_message()->sender();
        if (dropped_) {
            actor_zeta::send(dispatcher, address(), handler_id(route::update_finish), session,
                             result_update(context_->resource()));
            return;
        }

        auto plan = planner::create_plan(view(), logic_plan, components::ql::limit_t::unlimit());
        if (!plan) {
            actor_zeta::send(dispatcher, address(), handler_id(route::update_finish), session,
                             result_update(context_->resource()));
            return;
        }
        components::pipeline::context_t pipeline_context{session, address(), std::move(parameters)};
        plan->on_execute(&pipeline_context);
        if (!plan->is_executed()) {
            sessions::make_session(
                sessions_, session,
                sessions::suspend_plan_t{current_message()->sender(), std::move(plan), std::move(pipeline_context)});
            return;
        }
        if (plan->output()) {
            auto new_id = components::document::get_document_id(plan->output()->documents().front());
            result_update result{new_id, context_->resource()};
            std::pmr::vector<document_id_t> documents{context_->resource()};
            documents.emplace_back(new_id);
            actor_zeta::send(mdisk_, address(), disk::handler_id(disk::route::remove_documents), session,
                             std::string(name_.database), std::string(name_.collection), documents);
            actor_zeta::send(dispatcher, address(), handler_id(route::update_finish), session, std::move(result));
        } else {
            result_update result{plan->modified() ? plan->modified()->documents()
                                                  : std::pmr::vector<document_id_t>{context_->resource()},
                                 plan->modified() ? plan->no_modified()->documents()
                                                  : std::pmr::vector<document_id_t>{context_->resource()}};
            actor_zeta::send(mdisk_, address(), disk::handler_id(disk::route::remove_documents), session,
                             std::string(name_.database), std::string(name_.collection), result.modified_ids());
            actor_zeta::send(dispatcher, address(), handler_id(route::update_finish), session, std::move(result));
        }
    }

    void collection_t::drop(const session_id_t& session) {
        trace(log(), "collection::drop : {}", name_.to_string());
        auto dispatcher = current_message()->sender();
        actor_zeta::send(dispatcher, address(), handler_id(route::drop_collection_finish), session,
                         result_drop_collection(drop_()), name_.database, name_.collection);
    }

    std::size_t collection_t::size_impl_() const { return static_cast<size_t>(context_->storage().size()); }

    bool collection_t::drop_() {
        if (dropped_) {
            return false;
        }
        dropped_ = true;
        return true;
    }

    void collection_t::close_cursor(session_id_t& session) { cursor_storage_.erase(session); }

    context_collection_t* collection_t::view() const { return context_.get(); }

    context_collection_t* collection_t::extract() {
        auto* ptr = context_.release();
        dropped_ = true;
        context_ = nullptr;
        return ptr;
    }

    log_t& collection_t::log() noexcept { return context_->log(); }

#ifdef DEV_MODE

    std::size_t collection_t::size_test() const { return size_impl_(); }

#endif

} // namespace services::collection