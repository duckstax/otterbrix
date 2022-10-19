#include "wrapper_dispatcher.hpp"
#include "forward.hpp"
#include "route.hpp"
#include <core/system_command.hpp>

namespace duck_charmer {

    wrapper_dispatcher_t::wrapper_dispatcher_t(actor_zeta::detail::pmr::memory_resource* mr,actor_zeta::address_t manager_dispatcher,log_t &log)
        : actor_zeta::cooperative_supervisor<wrapper_dispatcher_t>(mr,"wrapper_dispatcher")
        , manager_dispatcher_(manager_dispatcher)
        ,log_(log.clone()) {
        add_handler(core::handler_id(core::route::load_finish), &wrapper_dispatcher_t::load_finish);
        add_handler(database::handler_id(database::route::create_database_finish), &wrapper_dispatcher_t::create_database_finish);
        add_handler(database::handler_id(database::route::create_collection_finish), &wrapper_dispatcher_t::create_collection_finish);
        add_handler(database::handler_id(database::route::drop_collection_finish), &wrapper_dispatcher_t::drop_collection_finish);
        add_handler(collection::handler_id(collection::route::insert_one_finish), &wrapper_dispatcher_t::insert_one_finish);
        add_handler(collection::handler_id(collection::route::insert_many_finish), &wrapper_dispatcher_t::insert_many_finish);
        add_handler(collection::handler_id(collection::route::find_finish), &wrapper_dispatcher_t::find_finish);
        add_handler(collection::handler_id(collection::route::find_one_finish), &wrapper_dispatcher_t::find_one_finish);
        add_handler(collection::handler_id(collection::route::delete_finish), &wrapper_dispatcher_t::delete_finish);
        add_handler(collection::handler_id(collection::route::update_finish), &wrapper_dispatcher_t::update_finish);
        add_handler(collection::handler_id(collection::route::size_finish), &wrapper_dispatcher_t::size_finish);
        add_handler(collection::handler_id(collection::route::create_index_finish), &wrapper_dispatcher_t::create_index_finish);
    }

    auto wrapper_dispatcher_t::load() -> void {
        session_id_t session;
        trace(log_, "wrapper_dispatcher_t::load session: {}", session.data());
        init();
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            core::handler_id(core::route::load),
            session);
        wait();
    }

    auto wrapper_dispatcher_t::create_database(session_id_t &session, const database_name_t &database) -> void {
        trace(log_, "wrapper_dispatcher_t::create_database session: {}, database name : {} ", session.data(), database);
        /// todo: trace(log_, "type address : {}", manager_dispatcher_->id());
        init();
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            database::handler_id(database::route::create_database),
            session,
            database);
        wait();
    }

    auto wrapper_dispatcher_t::create_collection(session_id_t &session, const database_name_t &database, const collection_name_t &collection) -> void {
        trace(log_, "wrapper_dispatcher_t::create_collection session: {}, database name : {} , collection name : {} ", session.data(), database, collection);
        /// todo: trace(log_, "type address : {}", manager_dispatcher_->id());
        init();
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            database::handler_id(database::route::create_collection),
            session,
            database,
            collection);
        wait();
    }

    auto wrapper_dispatcher_t::drop_collection(components::session::session_id_t &session, const database_name_t &database, const collection_name_t &collection) -> result_drop_collection {
        trace(log_, "wrapper_dispatcher_t::drop_collection session: {}, database name: {}, collection name: {} ", session.data(), database, collection);
        init();
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            database::handler_id(database::route::drop_collection),
            session,
            database,
            collection);
        wait();
        return std::get<result_drop_collection>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::insert_one(session_id_t &session, const database_name_t &database, const collection_name_t &collection, document_ptr &document) -> result_insert_one &{
        trace(log_, "wrapper_dispatcher_t::insert_one session: {}, collection name: {} ", session.data(), collection);
        init();
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            collection::handler_id(collection::route::insert_one),
            session,
            database,
            collection,
            std::move(document));
        wait();
        return std::get<result_insert_one>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::insert_many(session_id_t &session, const database_name_t &database, const collection_name_t &collection, std::pmr::vector<document_ptr> &documents) -> result_insert_many &{
        trace(log_, "wrapper_dispatcher_t::insert_many session: {}, collection name: {} ", session.data(), collection);
        init();
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            collection::handler_id(collection::route::insert_many),
            session,
            database,
            collection,
            std::move(documents));
        wait();
        return std::get<result_insert_many>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::find(session_id_t &session, const database_name_t &database, const collection_name_t &collection, document_ptr condition) -> components::cursor::cursor_t* {
        trace(log_, "wrapper_dispatcher_t::find session: {}, collection name : {} ", session.data(), collection);
        init();
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            collection::handler_id(collection::route::find),
            session,
            database,
            collection,
            std::move(condition));
        wait();
        return std::get<components::cursor::cursor_t*>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::find_one(components::session::session_id_t &session, const database_name_t &database, const collection_name_t &collection, document_ptr condition) -> result_find_one &{
        trace(log_, "wrapper_dispatcher_t::find_one session: {}, collection name : {} ", session.data(), collection);
        init();
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            collection::handler_id(collection::route::find_one),
            session,
            database,
            collection,
            std::move(condition));
        wait();
        return std::get<result_find_one>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::delete_one(components::session::session_id_t &session, const database_name_t &database, const collection_name_t &collection, document_ptr condition) -> result_delete &{
        trace(log_, "wrapper_dispatcher_t::delete_one session: {}, database: {} collection: {} ", session.data(), database, collection);
        init();
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            collection::handler_id(collection::route::delete_one),
            session,
            database,
            collection,
            std::move(condition));
        wait();
        return std::get<result_delete>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::delete_many(components::session::session_id_t &session, const database_name_t &database, const collection_name_t &collection, document_ptr condition) -> result_delete &{
        trace(log_, "wrapper_dispatcher_t::delete_many session: {}, database: {} collection: {} ", session.data(), database, collection);
        init();
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            collection::handler_id(collection::route::delete_many),
            session,
            database,
            collection,
            std::move(condition));
        wait();
        return std::get<result_delete>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::update_one(components::session::session_id_t &session, const database_name_t &database, const collection_name_t &collection, document_ptr condition, document_ptr update, bool upsert) -> result_update &{
        trace(log_, "wrapper_dispatcher_t::update_one session: {}, database: {} collection: {} ", session.data(), database, collection);
        init();
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            collection::handler_id(collection::route::update_one),
            session,
            database,
            collection,
            std::move(condition),
            std::move(update),
            upsert);
        wait();
        return std::get<result_update>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::update_many(components::session::session_id_t &session, const database_name_t &database, const collection_name_t &collection, document_ptr condition, document_ptr update, bool upsert) -> result_update &{
        trace(log_, "wrapper_dispatcher_t::update_many session: {}, database: {} collection: {} ", session.data(), database, collection);
        init();
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            collection::handler_id(collection::route::update_many),
            session,
            database,
            collection,
            std::move(condition),
            std::move(update),
            upsert);
        wait();
        return std::get<result_update>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::size(session_id_t &session, const database_name_t &database, const collection_name_t &collection) -> result_size {
        trace(log_, "wrapper_dispatcher_t::size session: {}, collection name : {} ", session.data(), collection);
        init();
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            collection::handler_id(collection::route::size),
            session,
            database,
            collection);
        wait();
        return std::get<result_size>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::create_index(session_id_t &session, components::ql::create_index_t index) -> result_create_index {
        trace(log_, "wrapper_dispatcher_t::create_index session: {}, database: {} collection: {} ", session.data(), index.database_, index.collection_);
        init();
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            collection::handler_id(collection::route::create_index),
            session,
            std::move(index));
        wait();
        return std::get<result_create_index>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* {
        throw std::runtime_error("wrapper_dispatcher_t::executor_impl");
    }

    auto wrapper_dispatcher_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
        std::unique_lock<spin_lock> _(input_mtx_);
        auto tmp = std::move(msg);
        trace(log_, "wrapper_dispatcher_t::enqueue_base msg type: {}", tmp->command().integer_value());
        set_current_message(std::move(tmp));
        execute(this,current_message());
    }

    auto wrapper_dispatcher_t::load_finish() -> void {
        trace(log_, "wrapper_dispatcher_t::load_finish");
        notify();
    }

    auto wrapper_dispatcher_t::create_database_finish(session_id_t &session, services::database::database_create_result result) -> void {
        trace(log_, "wrapper_dispatcher_t::create_database_finish session: {} , result: {} ", session.data(), result.created_);
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    auto wrapper_dispatcher_t::create_collection_finish(session_id_t &session, services::database::collection_create_result result) -> void {
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    void wrapper_dispatcher_t::drop_collection_finish(session_id_t &session, result_drop_collection result) {
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    auto wrapper_dispatcher_t::insert_one_finish(session_id_t &session, result_insert_one result) -> void {
        trace(log_, "wrapper_dispatcher_t::insert_one_finish session: {}, result: {} inserted", session.data(), result.inserted_id().is_null() ? 0 : 1);
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    void wrapper_dispatcher_t::insert_many_finish(session_id_t &session, result_insert_many result) {
        trace(log_, "wrapper_dispatcher_t::insert_many_finish session: {}, result: {} inserted", session.data(), result.inserted_ids().size());
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    auto wrapper_dispatcher_t::find_finish(session_id_t &session, components::cursor::cursor_t* cursor) -> void {
        intermediate_store_ = cursor;
        input_session_ = session;
        notify();
    }

    void wrapper_dispatcher_t::find_one_finish(session_id_t &session, result_find_one result) {
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    void wrapper_dispatcher_t::delete_finish(session_id_t &session, result_delete result) {
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    void wrapper_dispatcher_t::update_finish(session_id_t &session, result_update result) {
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    auto wrapper_dispatcher_t::size_finish(session_id_t &session, result_size result) -> void {
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    auto wrapper_dispatcher_t::create_index_finish(session_id_t &session, result_create_index result) -> void {
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    void wrapper_dispatcher_t::init() {
        i = 0;
    }

    void wrapper_dispatcher_t::wait() {
        std::unique_lock<std::mutex> lk(output_mtx_);
        cv_.wait(lk, [this]() { return i == 1; });
    }

    void wrapper_dispatcher_t::notify() {
        i = 1;
        cv_.notify_all();
    }

} // namespace duck_charmer