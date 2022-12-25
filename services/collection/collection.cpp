#include "collection.hpp"
#include <core/system_command.hpp>
#include <services/disk/route.hpp>
#include <services/collection/operators/scan/scan.hpp>
#include <services/collection/operators/scan/primary_key_scan.hpp>
#include <services/collection/operators/operator_insert.hpp>
#include <services/collection/operators/operator_delete.hpp>
#include <services/collection/operators/operator_update.hpp>

using namespace services::collection;

namespace services::collection {

    planner::transaction_context_t* no_transaction_context = nullptr; //todo: remove


    collection_t::collection_t(database::database_t* database, const std::string& name, log_t& log, actor_zeta::address_t mdisk)
        : actor_zeta::basic_async_actor(database, std::string(name))
        , name_(name)
        , database_name_(database->name()) //todo for run test [default: database->name()]
        , log_(log.clone())
        , database_(database ? database->address() : actor_zeta::address_t::empty_address()) //todo for run test [default: database->address()]
        , mdisk_(std::move(mdisk))
        , context_(std::make_unique<context_collection_t>(new std::pmr::monotonic_buffer_resource()))
        , cursor_storage_(context_->resource()) {
        add_handler(handler_id(route::create_documents), &collection_t::create_documents);
        add_handler(handler_id(route::insert_one), &collection_t::insert_one);
        add_handler(handler_id(route::insert_many), &collection_t::insert_many);
        add_handler(handler_id(route::find), &collection_t::find);
        add_handler(handler_id(route::find_one), &collection_t::find_one);
        add_handler(handler_id(route::delete_one), &collection_t::delete_one);
        add_handler(handler_id(route::delete_many), &collection_t::delete_many);
        add_handler(handler_id(route::update_one), &collection_t::update_one);
        add_handler(handler_id(route::update_many), &collection_t::update_many);
        add_handler(handler_id(route::size), &collection_t::size);
        add_handler(handler_id(route::drop_collection), &collection_t::drop);
        add_handler(handler_id(route::close_cursor), &collection_t::close_cursor);
        add_handler(handler_id(route::create_index), &collection_t::create_index);
    }

    void collection_t::create_documents(components::session::session_id_t& session, std::pmr::vector<document_ptr>& documents) {
        debug(log_, "{}::{}::create_documents, count: {}", database_name_, name_, documents.size());
        insert_(no_transaction_context, documents);
        actor_zeta::send(current_message()->sender(), address(), handler_id(route::create_documents_finish), session);
    }

    auto collection_t::size(session_id_t& session) -> void {
        debug(log_, "collection {}::size", name_);
        auto dispatcher = current_message()->sender();
        auto result = dropped_
                          ? result_size()
                          : result_size(size_());
        actor_zeta::send(dispatcher, address(), handler_id(route::size_finish), session, result);
    }

    void collection_t::insert_one(session_id_t& session, document_ptr& document) {
        debug(log_, "collection_t::insert_one : {}", name_);
        auto dispatcher = current_message()->sender();
        if (dropped_) {
            actor_zeta::send(dispatcher, address(), handler_id(route::insert_one_finish), session, result_insert_one());
        } else {
            std::pmr::vector<document_ptr> documents(context_->resource());
            documents.push_back(document);
            auto result = insert_(no_transaction_context, documents);
            if (!result.empty()) {
                actor_zeta::send(mdisk_, address(), disk::handler_id(disk::route::write_documents), session, std::string(database_name_), std::string(name_), documents);
                actor_zeta::send(dispatcher, address(), handler_id(route::insert_one_finish), session, result_insert_one(result.at(0)));
            } else {
                actor_zeta::send(dispatcher, address(), handler_id(route::insert_one_finish), session, result_insert_one());
            }
        }
    }

    void collection_t::insert_many(session_id_t& session, std::pmr::vector<document_ptr>& documents) {
        debug(log_, "collection_t::insert_many : {}", name_);
        auto dispatcher = current_message()->sender();
        if (dropped_) {
            actor_zeta::send(dispatcher, address(), handler_id(route::insert_many_finish), session, result_insert_many(context_->resource()));
        } else {
            auto result = insert_(no_transaction_context, documents);
            if (!result.empty()) {
                actor_zeta::send(mdisk_, address(), disk::handler_id(disk::route::write_documents), session, std::string(database_name_), std::string(name_), documents);
            }
            actor_zeta::send(dispatcher, address(), handler_id(route::insert_many_finish), session, result_insert_many(std::move(result)));
        }
    }

    auto collection_t::find(const session_id_t& session, const components::ql::aggregate_statement_ptr& cond) -> void {
//        debug(log_, "collection::find : {}", name_);
//        auto dispatcher = current_message()->sender();
//        if (dropped_) {
//            actor_zeta::send(dispatcher, address(), handler_id(route::find_finish), session, nullptr);
//        } else {
//            auto searcher = operators::create_searcher(view(), cond->condition_, operators::predicates::limit_t::unlimit());
//            searcher->on_execute(no_transaction_context);
//            auto result = cursor_storage_.emplace(session, std::make_unique<components::cursor::sub_cursor_t>(context_->resource(), address()));
//            if (searcher->output()) {
//                for (const auto &document : searcher->output()->documents()) {
//                    result.first->second->append(document_view_t(document));
//                }
//            }
//            actor_zeta::send(dispatcher, address(), handler_id(route::find_finish), session, result.first->second.get());
//        }
    }

    void collection_t::find_one(const session_id_t& session, const components::ql::aggregate_statement_ptr& cond) {
//        debug(log_, "collection::find_one : {}", name_);
//        auto dispatcher = current_message()->sender();
//        if (dropped_) {
//            actor_zeta::send(dispatcher, address(), handler_id(route::find_one_finish), session, nullptr);
//        } else {
//            auto searcher = operators::create_searcher(view(), cond->condition_, operators::predicates::limit_t::limit_one());
//            searcher->on_execute(no_transaction_context);
//            if (searcher->output() && !searcher->output()->documents().empty()) {
//                actor_zeta::send(dispatcher, address(), handler_id(route::find_one_finish), session, result_find_one(document_view_t(searcher->output()->documents().at(0))));
//            } else {
//                actor_zeta::send(dispatcher, address(), handler_id(route::find_one_finish), session, result_find_one());
//            }
//        }
    }

    auto collection_t::delete_one(const session_id_t& session, const components::ql::aggregate_statement_ptr& cond) -> void {
        debug(log_, "collection::delete_one : {}", name_);
        delete_(no_transaction_context, session, cond, operators::predicates::limit_t::limit_one());
    }

    auto collection_t::delete_many(const session_id_t& session, const components::ql::aggregate_statement_ptr& cond) -> void {
        debug(log_, "collection::delete_many : {}", name_);
        delete_(no_transaction_context, session, cond, operators::predicates::limit_t::unlimit());
    }

    auto collection_t::update_one(const session_id_t& session, const components::ql::aggregate_statement_ptr& cond, const document_ptr& update, bool upsert) -> void {
        debug(log_, "collection::update_one : {}", name_);
        update_(no_transaction_context, session, cond, update, upsert, operators::predicates::limit_t::limit_one());
    }

    auto collection_t::update_many(const session_id_t& session, const components::ql::aggregate_statement_ptr& cond, const document_ptr& update, bool upsert) -> void {
        debug(log_, "collection::update_many : {}", name_);
        update_(no_transaction_context, session, cond, update, upsert, operators::predicates::limit_t::unlimit());
    }

    void collection_t::drop(const session_id_t& session) {
        debug(log_, "collection::drop : {}", name_);
        auto dispatcher = current_message()->sender();
        actor_zeta::send(dispatcher, address(), handler_id(route::drop_collection_finish), session, result_drop_collection(drop_()), std::string(database_name_), std::string(name_));
    }

    std::pmr::vector<document_id_t> collection_t::insert_(planner::transaction_context_t* transaction_context, const std::pmr::vector<document_ptr>& documents) {
        auto searcher = std::make_unique<operators::primary_key_scan>(view());
        for (const auto &document : documents) {
            searcher->append(get_document_id(document));
        }
        operators::operator_insert inserter(view(), documents);
        inserter.set_children(std::move(searcher));
        inserter.on_execute(transaction_context);
        return inserter.modified() ? inserter.modified()->documents() : std::pmr::vector<document_id_t>();
    }

    std::size_t collection_t::size_() const {
        return static_cast<size_t>(context_->storage().size());
    }

    bool collection_t::drop_() {
        if (dropped_) {
            return false;
        }
        dropped_ = true;
        return true;
    }

    void collection_t::delete_(planner::transaction_context_t* transaction_context, const session_id_t& session, const components::ql::aggregate_statement_ptr& cond, const operators::predicates::limit_t &limit) {
//        auto dispatcher = current_message()->sender();
//        if (dropped_) {
//            actor_zeta::send(dispatcher, address(), handler_id(route::delete_finish), session, result_delete(context_->resource()));
//        } else {
//            operators::operator_delete deleter(view());
//            deleter.set_children(operators::create_searcher(view(), cond->condition_, limit));
//            deleter.on_execute(transaction_context);
//            if (deleter.modified()) {
//                result_delete result(std::move(deleter.modified()->documents()));
//                send_delete_to_disk_(session, result);
//                actor_zeta::send(dispatcher, address(), handler_id(route::delete_finish), session, result);
//            } else {
//                actor_zeta::send(dispatcher, address(), handler_id(route::delete_finish), session, result_delete(context_->resource()));
//            }
//        }
    }

    void collection_t::update_(planner::transaction_context_t* transaction_context, const session_id_t& session, const components::ql::aggregate_statement_ptr& cond, const document_ptr& update,
                               bool upsert, const operators::predicates::limit_t &limit) {
//        auto dispatcher = current_message()->sender();
//        if (dropped_) {
//            actor_zeta::send(dispatcher, address(), handler_id(route::update_finish), session, result_update(context_->resource()));
//        } else {
//            operators::operator_update updater(view(), update);
//            updater.set_children(operators::create_searcher(view(), cond->condition_, limit));
//            updater.on_execute(transaction_context);
//            if (updater.modified()) {
//                result_update result(std::move(updater.modified()->documents()), std::move(updater.no_modified()->documents()));
//                send_update_to_disk_(session, result);
//                actor_zeta::send(dispatcher, address(), handler_id(route::update_finish), session, result);
//            } else if (upsert) {
//                result_update result(get_document_id(update), view()->resource());
//                std::pmr::vector<document_ptr> documents(context_->resource());
//                documents.push_back(make_upsert_document(update));
//                insert_(transaction_context, documents);
//                actor_zeta::send(dispatcher, address(), handler_id(route::update_finish), session, result);
//            } else {
//                actor_zeta::send(dispatcher, address(), handler_id(route::update_finish), session, result_update(context_->resource()));
//            }
//        }
    }

    void collection_t::send_update_to_disk_(const session_id_t& session, const result_update& result) {
        std::pmr::vector<document_ptr> update_documents;
        for (const auto& id : result.modified_ids()) {
            update_documents.push_back(context_->storage().at(id));
        }
        if (!result.upserted_id().is_null()) {
            update_documents.push_back(context_->storage().at(result.upserted_id()));
        }
        if (!update_documents.empty()) {
            actor_zeta::send(mdisk_, address(), disk::handler_id(disk::route::write_documents), session, std::string(database_name_), std::string(name_), update_documents);
        }
    }

    void collection_t::send_delete_to_disk_(const session_id_t& session, const result_delete& result) {
        if (!result.empty()) {
            actor_zeta::send(mdisk_, address(), disk::handler_id(disk::route::remove_documents), session, std::string(database_name_), std::string(name_), result.deleted_ids());
        }
    }

    void collection_t::close_cursor(session_id_t& session) {
        cursor_storage_.erase(session);
    }

    context_collection_t* collection_t::view() const {
        return context_.get();
    }

    context_collection_t* collection_t::extract() {
        auto* ptr = context_.release();
        dropped_ = true;
        context_ = nullptr;
        return ptr;
    }

#ifdef DEV_MODE

    std::size_t collection_t::size_test() const {
        return size_();
    }

#endif

} // namespace services::collection
