#include "wrapper_dispatcher.hpp"
#include "forward.hpp"
#include "route.hpp"

void spin_lock::lock() {
    while (_lock.test_and_set(std::memory_order_acquire)) continue;
}
void spin_lock::unlock() {
    _lock.clear(std::memory_order_release);
}

namespace duck_charmer {

    wrapper_dispatcher_t::wrapper_dispatcher_t(log_t &log)
        : manager_t("wrapper_dispatcher")
        , log_(log.clone()) {
        add_handler(database::create_database_finish, &wrapper_dispatcher_t::create_database_finish);
        add_handler(database::create_collection_finish, &wrapper_dispatcher_t::create_collection_finish);
        add_handler(database::drop_collection_finish, &wrapper_dispatcher_t::drop_collection_finish);
        add_handler(collection::insert_one_finish, &wrapper_dispatcher_t::insert_one_finish);
        add_handler(collection::insert_many_finish, &wrapper_dispatcher_t::insert_many_finish);
        add_handler(collection::find_finish, &wrapper_dispatcher_t::find_finish);
        add_handler(collection::find_one_finish, &wrapper_dispatcher_t::find_one_finish);
        add_handler(collection::delete_finish, &wrapper_dispatcher_t::delete_finish);
        add_handler(collection::update_finish, &wrapper_dispatcher_t::update_finish);
        add_handler(collection::size_finish, &wrapper_dispatcher_t::size_finish);
    }

    auto wrapper_dispatcher_t::create_database(session_id_t &session, const database_name_t &database) -> void {
        trace(log_, "wrapper_dispatcher_t::create_database session: {}, database name : {} ", session.data(), database);
        trace(log_, "type address : {}", address_book("manager_dispatcher").type());
        init();
        goblin_engineer::send(
            address_book("manager_dispatcher"),
            address(),
            database::create_database,
            session,
            database);
        wait();
    }

    auto wrapper_dispatcher_t::create_collection(session_id_t &session, const database_name_t &database, const collection_name_t &collection) -> void {
        trace(log_, "wrapper_dispatcher_t::create_collection session: {}, database name : {} , collection name : {} ", session.data(), database, collection);
        trace(log_, "type address : {}", address_book("manager_dispatcher").type());
        init();
        goblin_engineer::send(
            address_book("manager_dispatcher"),
            address(),
            database::create_collection,
            session,
            database,
            collection);
        wait();
    }

    auto wrapper_dispatcher_t::drop_collection(components::session::session_id_t &session, const database_name_t &database, const collection_name_t &collection) -> result_drop_collection {
        trace(log_, "wrapper_dispatcher_t::drop_collection session: {}, database name: {}, collection name: {} ", session.data(), database, collection);
        init();
        goblin_engineer::send(
            address_book("manager_dispatcher"),
            address(),
            database::drop_collection,
            session,
            database,
            collection);
        wait();
        return std::get<result_drop_collection>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::insert_one(session_id_t &session, const database_name_t &database, const collection_name_t &collection, document_ptr &document) -> result_insert_one &{
        trace(log_, "wrapper_dispatcher_t::insert_one session: {}, collection name: {} ", session.data(), collection);
        init();
        goblin_engineer::send(
            address_book("manager_dispatcher"),
            address(),
            collection::insert_one,
            session,
            database,
            collection,
            std::move(document));
        wait();
        return std::get<result_insert_one>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::insert_many(session_id_t &session, const database_name_t &database, const collection_name_t &collection, std::list<document_ptr> &documents) -> result_insert_many &{
        trace(log_, "wrapper_dispatcher_t::insert_many session: {}, collection name: {} ", session.data(), collection);
        init();
        goblin_engineer::send(
            address_book("manager_dispatcher"),
            address(),
            collection::insert_many,
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
        goblin_engineer::send(
            address_book("manager_dispatcher"),
            address(),
            collection::find,
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
        goblin_engineer::send(
            address_book("manager_dispatcher"),
            address(),
            collection::find_one,
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
        goblin_engineer::send(
            address_book("manager_dispatcher"),
            address(),
            collection::delete_one,
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
        goblin_engineer::send(
            address_book("manager_dispatcher"),
            address(),
            collection::delete_many,
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
        goblin_engineer::send(
            address_book("manager_dispatcher"),
            address(),
            collection::update_one,
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
        goblin_engineer::send(
            address_book("manager_dispatcher"),
            address(),
            collection::update_many,
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
        goblin_engineer::send(
            address_book("manager_dispatcher"),
            address(),
            collection::size,
            session,
            database,
            collection);
        wait();
        return std::get<result_size>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::add_actor_impl(goblin_engineer::actor) -> void {
        throw std::runtime_error("wrapper_dispatcher_t::add_actor_impl");
    }
    auto wrapper_dispatcher_t::add_supervisor_impl(goblin_engineer::supervisor) -> void {
        throw std::runtime_error("wrapper_dispatcher_t::add_supervisor_impl");
    }

    auto wrapper_dispatcher_t::executor_impl() noexcept -> goblin_engineer::abstract_executor* {
        throw std::runtime_error("wrapper_dispatcher_t::executor_impl");
    }

    auto wrapper_dispatcher_t::enqueue_base(actor_zeta::message_ptr msg, actor_zeta::execution_device*) -> void {
        std::unique_lock<spin_lock> _(input_mtx_);
        auto tmp = std::move(msg);
        trace(log_, "wrapper_dispatcher_t::enqueue_base msg type: {}", tmp->command());
        set_current_message(std::move(tmp));
        execute();
    }

    auto wrapper_dispatcher_t::create_database_finish(session_id_t &session, services::storage::database_create_result result) -> void {
        trace(log_, "wrapper_dispatcher_t::create_database_finish session: {} , result: {} ", session.data(), result.created_);
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    auto wrapper_dispatcher_t::create_collection_finish(session_id_t &session, services::storage::collection_create_result result) -> void {
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