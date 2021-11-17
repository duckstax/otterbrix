#include "wrapper_dispatcher.hpp"
#include "wrapper_cursor.hpp"
#include "forward.hpp"
#include "route.hpp"
#include "services/storage/result_database.hpp"
#include "wrapper_collection.hpp"
#include "wrapper_database.hpp"


namespace duck_charmer {

    wrapper_dispatcher_t::wrapper_dispatcher_t(log_t& log,const std::string& name_dispather )
        : manager_t("wrapper_dispatcher")
        , log_(log.clone())
        , name_(name_dispather) {
        add_handler("create_database_finish", &wrapper_dispatcher_t::create_database_finish);
        add_handler("create_collection_finish", &wrapper_dispatcher_t::create_collection_finish);
        add_handler("drop_collection_finish", &wrapper_dispatcher_t::drop_collection_finish);
        add_handler("insert_one_finish", &wrapper_dispatcher_t::insert_one_finish);
        add_handler("insert_many_finish", &wrapper_dispatcher_t::insert_many_finish);
        add_handler("find_finish", &wrapper_dispatcher_t::find_finish);
        add_handler("find_one_finish", &wrapper_dispatcher_t::find_one_finish);
        add_handler("size_finish", &wrapper_dispatcher_t::size_finish);
    }

    auto wrapper_dispatcher_t::create_database(duck_charmer::session_t& session, const std::string& name) -> wrapper_database_ptr {
        log_.trace("wrapper_dispatcher_t::create_database session: {}, database name : {} ", session.data(), name);
        log_.trace("type address : {}",  address_book("manager_dispatcher").type());
        init();
        goblin_engineer::send(
            address_book("manager_dispatcher"),
            address(),
            "create_database",
            session,
            name);
        wait();
        auto result = std::get<services::storage::database_create_result>(intermediate_store_);
        return wrapper_database_ptr(new wrapper_database(name, this, log_));
    }

    auto wrapper_dispatcher_t::create_collection(duck_charmer::session_t& session, const std::string& database_name, const std::string& collection_name) -> wrapper_collection_ptr {
        log_.trace("wrapper_dispatcher_t::create_collection session: {}, database name : {} , collection name : {} ", session.data(), database_name,collection_name);
        log_.trace("type address : {}",  address_book("manager_dispatcher").type());
        init();
        goblin_engineer::send(
            address_book("manager_dispatcher"),
            address(),
            "create_collection",
            session,
            database_name,
            collection_name);
        wait();
        auto result =  std::get<services::storage::collection_create_result>(intermediate_store_);
        return wrapper_collection_ptr(new wrapper_collection(collection_name,this,log_));
    }

    result_drop_collection wrapper_dispatcher_t::drop_collection(components::session::session_t &session, const std::string &database, const std::string &collection) {
        log_.trace("wrapper_dispatcher_t::drop_collection session: {}, database name: {}, collection name: {} ", session.data(), database, collection);
        init();
        goblin_engineer::send(
            address_book("manager_dispatcher"),
            address(),
            database::drop_collection,
            session,
            database,
            collection
            );
        wait();
        return std::get<result_drop_collection>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::insert_one(duck_charmer::session_t& session, const std::string& collection, components::document::document_t &document) -> result_insert_one& {
        log_.trace("wrapper_dispatcher_t::insert_one session: {}, collection name: {} ", session.data(), collection);
        init();
        goblin_engineer::send(
            address_book("manager_dispatcher"),
            address(),
            collection::insert_one,
            session,
            collection,
            std::move(document));
        wait();
        return std::get<result_insert_one>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::insert_many(duck_charmer::session_t& session, const std::string& collection, std::list<components::document::document_t> &documents) -> result_insert_many& {
        log_.trace("wrapper_dispatcher_t::insert_many session: {}, collection name: {} ", session.data(), collection);
        init();
        goblin_engineer::send(
            address_book("manager_dispatcher"),
            address(),
            collection::insert_many,
            session,
            collection,
            std::move(documents));
        wait();
        return std::get<result_insert_many>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::find(duck_charmer::session_t& session,const std::string& collection, components::document::document_t condition) -> wrapper_cursor_ptr {
        log_.trace("wrapper_dispatcher_t::find session: {}, collection name : {} ", session.data(), collection);
        init();
        goblin_engineer::send(
            address_book("manager_dispatcher"),
            address(),
            collection::find,
            session,
            collection,
            std::move(condition)
            );
        wait();
        return wrapper_cursor_ptr(new wrapper_cursor(session, std::get<components::cursor::cursor_t*>(intermediate_store_)));
    }

    auto wrapper_dispatcher_t::find_one(components::session::session_t &session, const std::string &collection, components::document::document_t condition) -> result_find_one& {
        log_.trace("wrapper_dispatcher_t::find_one session: {}, collection name : {} ", session.data(), collection);
        init();
        goblin_engineer::send(
            address_book("manager_dispatcher"),
            address(),
            collection::find_one,
            session,
            collection,
            std::move(condition)
            );
        wait();
        return std::get<result_find_one>(intermediate_store_);
    }

    result_size wrapper_dispatcher_t::size(duck_charmer::session_t& session,const  std::string& collection) {
        log_.trace("wrapper_dispatcher_t::size session: {}, collection name : {} ", session.data(), collection);
        init();
        goblin_engineer::send(
            address_book("manager_dispatcher"),
            address(),
            collection::size,
            session,
            collection
            );
        wait();
        return std::get<result_size>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::create_database_finish(duck_charmer::session_t& session,services::storage::database_create_result result) -> void {
        log_.trace("wrapper_dispatcher_t::create_database_finish session: {} , result: {} ",session.data(),result.created_);
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    auto wrapper_dispatcher_t::create_collection_finish(duck_charmer::session_t& session,services::storage::collection_create_result result ) -> void {
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    void wrapper_dispatcher_t::drop_collection_finish(components::session::session_t &session, result_drop_collection result) {
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    auto wrapper_dispatcher_t::insert_one_finish(duck_charmer::session_t& session,result_insert_one result) -> void {
        log_.trace("wrapper_dispatcher_t::insert_one_finish session: {}, result: {} inserted", session.data(), result.inserted_id().size());
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    void wrapper_dispatcher_t::insert_many_finish(components::session::session_t &session, result_insert_many result) {
        log_.trace("wrapper_dispatcher_t::insert_many_finish session: {}, result: {} inserted", session.data(), result.inserted_ids().size());
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    auto wrapper_dispatcher_t::find_finish(duck_charmer::session_t& session,components::cursor::cursor_t* cursor) -> void {
        intermediate_store_ = cursor;
        input_session_ = session;
        notify();
    }

    void wrapper_dispatcher_t::find_one_finish(components::session::session_t &session, result_find_one result) {
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    auto wrapper_dispatcher_t::size_finish(duck_charmer::session_t& session,result_size result) -> void {
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

}