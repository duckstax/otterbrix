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

using namespace components::cursor;

namespace services::collection {

    collection_t::collection_t(services::memory_storage_t* memory_storage,
                               const collection_full_name_t& name,
                               log_t& log,
                               actor_zeta::address_t mdisk)
        : actor_zeta::basic_async_actor(memory_storage, std::string(name.to_string()))
        , name_(name)
        , mdisk_(std::move(mdisk))
        , context_(std::make_unique<context_collection_t>(new std::pmr::monotonic_buffer_resource(), log.clone()))
        , cursor_storage_(context_->resource()) {
        add_handler(handler_id(route::create_documents), &collection_t::create_documents);
        add_handler(handler_id(route::execute_plan), &collection_t::execute_plan);
        add_handler(handler_id(route::size), &collection_t::size);
        add_handler(handler_id(route::drop_collection), &collection_t::drop);
        add_handler(handler_id(route::close_cursor), &collection_t::close_cursor);
        add_handler(handler_id(route::create_index), &collection_t::create_index);
        add_handler(handler_id(index::route::success_create), &collection_t::create_index_finish);
        add_handler(handler_id(route::drop_index), &collection_t::drop_index);
        add_handler(handler_id(index::route::success), &collection_t::index_modify_finish);
        add_handler(handler_id(index::route::success_find), &collection_t::index_find_finish);
    }

    collection_t::~collection_t() { trace(log(), "delete collection_t"); }

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
        if (dropped_) {
            actor_zeta::send(dispatcher,
                             address(),
                             handler_id(route::size_finish),
                             session,
                             make_cursor(context_->resource(), error_code_t::collection_dropped));
        } else {
            auto* sub_cursor = new sub_cursor_t(context_->resource(), address());
            for (const auto& doc : context_->storage()) {
                sub_cursor->append(document_view_t(doc.second));
            }
            auto cursor = make_cursor(context_->resource());
            cursor->push(sub_cursor);
            actor_zeta::send(dispatcher, address(), handler_id(route::size_finish), session, cursor);
        }
    }

    auto collection_t::execute_plan(const components::session::session_id_t& session,
                                    const components::logical_plan::node_ptr& logical_plan,
                                    components::ql::storage_parameters parameters) -> void {
        trace(log(), "collection::execute_plan : {}, session {}", name_.to_string(), session.data());
        auto sender = current_message()->sender();
        if (dropped_) {
            actor_zeta::send(sender,
                             address(),
                             handler_id(route::execute_plan_finish),
                             session,
                             make_cursor(context_->resource(), error_code_t::collection_dropped, "collection dropped"));
            return;
        }
        auto plan = planner::create_plan(view(), logical_plan, components::ql::limit_t::unlimit());
        if (!plan) {
            actor_zeta::send(
                sender,
                address(),
                handler_id(route::execute_plan_finish),
                session,
                make_cursor(context_->resource(), error_code_t::create_phisical_plan_error, "invalid query plan"));
            return;
        }
        components::pipeline::context_t pipeline_context{session, address(), parameters};
        plan->on_execute(&pipeline_context);
        if (!plan->is_executed()) {
            sessions::make_session(sessions_,
                                   session,
                                   sessions::suspend_plan_t{sender, std::move(plan), std::move(pipeline_context)});
            return;
        }

        switch (plan->type()) {
            case operators::operator_type::insert: {
                insert_document_impl(session, sender, std::move(plan));
                return;
            }
            case operators::operator_type::remove: {
                delete_document_impl(session, sender, std::move(plan));
                return;
            }
            case operators::operator_type::update: {
                update_document_impl(session, sender, std::move(plan));
                return;
            }
            case operators::operator_type::aggregate: {
                // this is find/serach
                find_document_impl(session, sender, std::move(plan));
                return;
            }

            default: {
                // should never happen
                assert(false && "Problem with operator_type in collections");
                throw std::logic_error("Problem with operator_type in collections");
                return;
            }
        }
    }
    void collection_t::insert_document_impl(const components::session::session_id_t& session,
                                            const actor_zeta::address_t& sender,
                                            operators::operator_ptr plan) {
        trace(log(), "collection::execute_plan : operators::operator_type::insert");
        actor_zeta::send(mdisk_,
                         address(),
                         disk::handler_id(disk::route::write_documents),
                         session,
                         std::string(name_.database),
                         std::string(name_.collection),
                         plan->output() ? plan->output()->documents()
                                        : std::pmr::vector<document_ptr>{context_->resource()});

        auto cursor = make_cursor(context_->resource());
        auto* sub_cursor = new sub_cursor_t(context_->resource(), address());
        if (plan->modified()) {
            for (const auto& id : plan->modified()->documents()) {
                sub_cursor->append(document_view_t(context_->storage().at(id)));
            }
        } else {
            for (const auto& doc : context_->storage()) {
                sub_cursor->append(document_view_t(doc.second));
            }
        }
        cursor->push(sub_cursor);
        actor_zeta::send(sender, address(), handler_id(route::execute_plan_finish), session, cursor);
    }

    void collection_t::update_document_impl(const components::session::session_id_t& session,
                                            const actor_zeta::address_t& sender,
                                            operators::operator_ptr plan) {
        trace(log(), "collection::execute_plan : operators::operator_type::update");

        if (plan->output()) {
            auto new_id = components::document::get_document_id(plan->output()->documents().front());
            std::pmr::vector<document_id_t> documents{context_->resource()};
            documents.emplace_back(new_id);
            actor_zeta::send(mdisk_,
                             address(),
                             disk::handler_id(disk::route::remove_documents),
                             session,
                             std::string(name_.database),
                             std::string(name_.collection),
                             documents);
            auto cursor(new cursor_t(context_->resource()));
            auto* sub_cursor = new sub_cursor_t(context_->resource(), address());
            for (const auto& id : documents) {
                sub_cursor->append(document_view_t(context_->storage().at(id)));
            }
            cursor->push(sub_cursor);
            actor_zeta::send(sender, address(), handler_id(route::execute_plan_finish), session, cursor);
        } else {
            if (plan->modified()) {
                auto cursor(new cursor_t(context_->resource()));
                auto* sub_cursor =
                    new sub_cursor_t(plan->modified()->documents().get_allocator().resource(), address());
                for (const auto& id : plan->modified()->documents()) {
                    sub_cursor->append(document_view_t(context_->storage().at(id)));
                }
                cursor->push(sub_cursor);
                actor_zeta::send(mdisk_,
                                 address(),
                                 disk::handler_id(disk::route::remove_documents),
                                 session,
                                 std::string(name_.database),
                                 std::string(name_.collection),
                                 plan->modified()->documents());
                actor_zeta::send(sender, address(), handler_id(route::execute_plan_finish), session, cursor);
            } else {
                auto cursor(new cursor_t(context_->resource()));
                auto* sub_cursor = new sub_cursor_t(context_->resource(), address());
                cursor->push(sub_cursor);
                actor_zeta::send(mdisk_,
                                 address(),
                                 disk::handler_id(disk::route::remove_documents),
                                 session,
                                 std::string(name_.database),
                                 std::string(name_.collection),
                                 std::pmr::vector<document_id_t>{context_->resource()});
                actor_zeta::send(sender, address(), handler_id(route::execute_plan_finish), session, cursor);
            }
        }
    }

    void collection_t::delete_document_impl(const components::session::session_id_t& session,
                                            const actor_zeta::address_t& sender,
                                            operators::operator_ptr plan) {
        trace(log(), "collection::execute_plan : operators::operator_type::remove");

        auto modified =
            plan->modified() ? plan->modified()->documents() : std::pmr::vector<document_id_t>{context_->resource()};
        actor_zeta::send(mdisk_,
                         address(),
                         disk::handler_id(disk::route::remove_documents),
                         session,
                         std::string(name_.database),
                         std::string(name_.collection),
                         modified);
        auto* sub_cursor = new sub_cursor_t(plan->modified()->documents().get_allocator().resource(), address());
        for (const auto& id : plan->modified()->documents()) {
            sub_cursor->append(document_view_t(nullptr));
        }
        auto cursor = make_cursor(context_->resource());
        cursor->push(sub_cursor);
        actor_zeta::send(sender, address(), handler_id(route::execute_plan_finish), session, cursor);
    }

    void collection_t::find_document_impl(const components::session::session_id_t& session,
                                          const actor_zeta::address_t& sender,
                                          operators::operator_ptr plan) {
        trace(log(), "collection::execute_plan : operators::operator_type::agreggate");
        auto cursor = cursor_storage_.emplace(
            session,
            std::make_unique<components::cursor::sub_cursor_t>(context_->resource(), address()));
        if (plan->output()) {
            for (const auto& document : plan->output()->documents()) {
                cursor.first->second->append(document_view_t(document));
            }
        }
        if (cursor.first->second.get()->size() == 0) {
            actor_zeta::send(sender,
                             address(),
                             handler_id(route::execute_plan_finish),
                             session,
                             make_cursor(context_->resource(), operation_status_t::failure));
        } else {
            auto result = make_cursor(context_->resource(), operation_status_t::success);
            result->push(cursor.first->second.get());
            actor_zeta::send(sender, address(), handler_id(route::execute_plan_finish), session, result);
        }
    }

    void collection_t::drop(const session_id_t& session) {
        trace(log(), "collection::drop : {}", name_.to_string());
        auto dispatcher = current_message()->sender();
        actor_zeta::send(
            dispatcher,
            address(),
            handler_id(route::drop_collection_finish),
            session,
            make_cursor(context_->resource(), drop_() ? operation_status_t::success : operation_status_t::failure),
            name_.database,
            name_.collection);
    }

    std::size_t collection_t::size_() const { return static_cast<size_t>(context_->storage().size()); }

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

    std::size_t collection_t::size_test() const { return size_(); }

#endif

} // namespace services::collection
