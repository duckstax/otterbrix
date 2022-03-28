#include "spaces.hpp"
#include <services/database/database.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <apps/duck_charmer/route.hpp>

using namespace duck_charmer;

namespace test {

    constexpr static char* name_dispatcher = "dispatcher";

    spaces_t& spaces_t::get() {
        static spaces_t spaces;
        return spaces;
    }

    log_t& spaces_t::log() {
        return log_;
    }

    void spaces_t::create_database(const database_name_t &database) {
        auto session = components::session::session_id_t();
        trace(log_, "spaces::create_database session: {}, database : {} ", session.data(), database);
        init();
        goblin_engineer::send(manager_dispatcher_, address(), manager_database::create_database, session, database);
        wait();
    }

    void spaces_t::create_collection(const database_name_t &database, const collection_name_t &collection) {
        auto session = components::session::session_id_t();
        trace(log_, "spaces::create_collection session: {}, database : {}, collection : {} ", session.data(), database, collection);
        init();
        goblin_engineer::send(manager_dispatcher_, address(), database::create_collection, session, database, collection);
        wait();
    }

    void spaces_t::drop_collection(const database_name_t &database, const collection_name_t &collection) {
        auto session = components::session::session_id_t();
        trace(log_, "spaces::drop_collection session: {}, database : {}, collection : {} ", session.data(), database, collection);
        init();
        goblin_engineer::send(manager_dispatcher_, address(), database::drop_collection, session, database, collection);
        wait();
    }

    void spaces_t::insert_one(const database_name_t& database, const collection_name_t& collection, components::document::document_ptr &document) {
        auto session = components::session::session_id_t();
        trace(log_, "spaces::insert_one session: {}, database : {}, collection : {} ", session.data(), database, collection);
        init();
        goblin_engineer::send(manager_dispatcher_, address(), collection::insert_one, session, database, collection, std::move(document));
        wait();
    }

    void spaces_t::insert_many(const database_name_t& database, const collection_name_t& collection, std::list<components::document::document_ptr> &documents) {
        auto session = components::session::session_id_t();
        trace(log_, "spaces::insert_many session: {}, database : {}, collection : {} ", session.data(), database, collection);
        init();
        goblin_engineer::send(manager_dispatcher_, address(), collection::insert_many, session, database, collection, std::move(documents));
        wait();
    }

    void spaces_t::find(const database_name_t& database, const collection_name_t& collection, components::document::document_ptr condition) {
        auto session = components::session::session_id_t();
        trace(log_, "spaces::find session: {}, database : {}, collection : {} ", session.data(), database, collection);
        init();
        goblin_engineer::send(manager_dispatcher_, address(), collection::find, session, database, collection, std::move(condition));
        wait();
    }

    void spaces_t::find_one(const database_name_t& database, const collection_name_t& collection, components::document::document_ptr condition) {
        auto session = components::session::session_id_t();
        trace(log_, "spaces::find_one session: {}, database : {}, collection : {} ", session.data(), database, collection);
        init();
        goblin_engineer::send(manager_dispatcher_, address(), collection::find_one, session, database, collection, std::move(condition));
        wait();
    }

    void spaces_t::delete_one(const database_name_t& database, const collection_name_t& collection, components::document::document_ptr condition) {
        auto session = components::session::session_id_t();
        trace(log_, "spaces::delete_one session: {}, database : {}, collection : {} ", session.data(), database, collection);
        init();
        goblin_engineer::send(manager_dispatcher_, address(), collection::delete_one, session, database, collection, std::move(condition));
        wait();
    }

    void spaces_t::delete_many(const database_name_t& database, const collection_name_t& collection, components::document::document_ptr condition) {
        auto session = components::session::session_id_t();
        trace(log_, "spaces::delete_many session: {}, database : {}, collection : {} ", session.data(), database, collection);
        init();
        goblin_engineer::send(manager_dispatcher_, address(), collection::delete_many, session, database, collection, std::move(condition));
        wait();
    }

    void spaces_t::update_one(const database_name_t& database, const collection_name_t& collection, components::document::document_ptr condition, components::document::document_ptr update, bool upsert) {
        auto session = components::session::session_id_t();
        trace(log_, "spaces::update_one session: {}, database : {}, collection : {} ", session.data(), database, collection);
        init();
        goblin_engineer::send(manager_dispatcher_, address(), collection::update_one, session, database, collection, std::move(condition), std::move(update), upsert);
        wait();
    }

    void spaces_t::update_many(const database_name_t& database, const collection_name_t& collection, components::document::document_ptr condition, components::document::document_ptr update, bool upsert) {
        auto session = components::session::session_id_t();
        trace(log_, "spaces::update_many session: {}, database : {}, collection : {} ", session.data(), database, collection);
        init();
        goblin_engineer::send(manager_dispatcher_, address(), collection::update_many, session, database, collection, std::move(condition), std::move(update), upsert);
        wait();
    }

    size_t spaces_t::size(const database_name_t& database, const collection_name_t& collection) {
        auto session = components::session::session_id_t();
        trace(log_, "spaces::size session: {}, database : {}, collection : {} ", session.data(), database, collection);
        init();
        goblin_engineer::send(manager_dispatcher_, address(), collection::size, session, database, collection);
        wait();
        return *std::get<result_size>(intermediate_store_);
    }

    auto spaces_t::add_actor_impl(goblin_engineer::actor) -> void {
        throw std::runtime_error("spaces::add_actor_impl");
    }
    auto spaces_t::add_supervisor_impl(goblin_engineer::supervisor) -> void {
        throw std::runtime_error("spaces::add_supervisor_impl");
    }

    auto spaces_t::executor_impl() noexcept -> goblin_engineer::abstract_executor* {
        throw std::runtime_error("spaces::executor_impl");
    }

    auto spaces_t::enqueue_base(actor_zeta::message_ptr msg, actor_zeta::execution_device*) -> void {
        std::unique_lock<spin_lock> _(input_mtx_);
        auto tmp = std::move(msg);
        set_current_message(std::move(tmp));
        execute();
    }

    spaces_t::spaces_t()
        : manager_t("spaces") {
        std::string log_dir("/tmp/");
        log_ = initialization_logger("duck_charmer", log_dir);
        log_.set_level(log_t::level::trace);
        boost::filesystem::path current_path = boost::filesystem::current_path();
        trace(log_, "spaces start");

        trace(log_, "manager_wal start");
        manager_wal_ = goblin_engineer::make_manager_service<manager_wal_replicate_t>(current_path,log_, 1, 1000);
        goblin_engineer::send(manager_wal_, goblin_engineer::address_t::empty_address(), "create");
        trace(log_, "manager_wal finish");

        trace(log_, "manager_database start");
        manager_database_ = goblin_engineer::make_manager_service<services::storage::manager_database_t>(log_, 1, 1000);
        trace(log_, "manager_database finish");

        trace(log_, "manager_dispatcher start");
        manager_dispatcher_ = goblin_engineer::make_manager_service<services::dispatcher::manager_dispatcher_t>(log_, 1, 1000);
        trace(log_, "manager_dispatcher finish");

        goblin_engineer::link(manager_wal_,manager_database_);
        goblin_engineer::link(manager_wal_,manager_dispatcher_);
        goblin_engineer::link(manager_database_, manager_dispatcher_);
        goblin_engineer::send(manager_dispatcher_, goblin_engineer::address_t::empty_address(), "create", components::session::session_id_t(), std::string(name_dispatcher));
        trace(log_, "manager_dispatcher create dispatcher");

        add_handler("create_database_finish", &spaces_t::create_database_finish);
        add_handler("create_collection_finish", &spaces_t::create_collection_finish);
        add_handler("drop_collection_finish", &spaces_t::drop_collection_finish);
        add_handler("insert_one_finish", &spaces_t::insert_one_finish);
        add_handler("insert_many_finish", &spaces_t::insert_many_finish);
        add_handler("find_finish", &spaces_t::find_finish);
        add_handler("find_one_finish", &spaces_t::find_one_finish);
        add_handler("delete_finish", &spaces_t::delete_finish);
        add_handler("update_finish", &spaces_t::update_finish);
        add_handler("size_finish", &spaces_t::size_finish);

        trace(log_, "spaces finish");
    }

    auto spaces_t::create_database_finish(session_id_t &session, services::storage::database_create_result result) -> void {
        log_.trace("spaces::create_database_finish session: {} , result: {} ", session.data(), result.created_);
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    auto spaces_t::create_collection_finish(session_id_t &session,services::storage::collection_create_result result) -> void {
        log_.trace("spaces::create_collection_finish session: {} , result: {} ", session.data(), result.created_);
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    auto spaces_t::drop_collection_finish(session_id_t &session, result_drop_collection result) -> void {
        log_.trace("spaces::drop_collection_finish session: {} , result: {} ", session.data(), result.is_success());
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    auto spaces_t::insert_one_finish(session_id_t &session, result_insert_one result) -> void {
        log_.trace("spaces::insert_one_finish session: {}, result: {} inserted", session.data(), result.inserted_id().is_null() ? 0 : 1);
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    auto spaces_t::insert_many_finish(session_id_t &session, result_insert_many result) -> void {
        log_.trace("spaces::insert_many_finish session: {}, result: {} inserted", session.data(), result.inserted_ids().size());
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    auto spaces_t::find_finish(session_id_t &session,components::cursor::cursor_t *cursor) -> void {
        log_.trace("spaces::find_finish session: {}, result: {} found", session.data(), cursor->size());
        intermediate_store_ = cursor;
        input_session_ = session;
        notify();
    }

    auto spaces_t::find_one_finish(session_id_t &session, result_find_one result) -> void {
        log_.trace("spaces::find_one_finish session: {}, result: {}found", session.data(), result.is_find() ? "" : "not ");
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    auto spaces_t::delete_finish(session_id_t &session, result_delete result) -> void {
        log_.trace("spaces::delete_finish session: {}, result: {} deleted", session.data(), result.deleted_ids().size());
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    auto spaces_t::update_finish(session_id_t &session, result_update result) -> void {
        log_.trace("spaces::update_finish session: {}, result: {} modified, {} unmodified, {}", session.data(),
                   result.modified_ids().size(), result.nomodified_ids().size(), result.upserted_id().is_null() ? "no upsert" : "upsert");
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    auto spaces_t::size_finish(session_id_t &session, result_size result) -> void {
        log_.trace("spaces::size_finish session: {}, result: {}", session.data(), *result);
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    void spaces_t::init() {
        i = 0;
    }

    void spaces_t::wait() {
        std::unique_lock<std::mutex> lk(output_mtx_);
        cv_.wait(lk, [this]() { return i == 1; });
    }

    void spaces_t::notify() {
        i = 1;
        cv_.notify_all();
    }

} //namespace test
