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
        add_handler(duck_charmer::database::create_collection, &wrapper_dispatcher_t::create_collection_finish);
        add_handler(duck_charmer::collection::insert, &wrapper_dispatcher_t::insert_finish);
        add_handler(duck_charmer::collection::find, &wrapper_dispatcher_t::find_finish);
        add_handler(duck_charmer::collection::size, &wrapper_dispatcher_t::size_finish);
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

    auto wrapper_dispatcher_t::create_collection(duck_charmer::session_t& session, const std::string& name) -> wrapper_collection_ptr {
        init();
        goblin_engineer::send(
            address_book(name_),
            address(),
            "create_collection",
            session,
            name);
        wait();
        auto result =  std::get<services::storage::collection_create_result>(intermediate_store_);
        return wrapper_collection_ptr(new wrapper_collection(name,this,log_));
    }

    result_insert_one& wrapper_dispatcher_t::insert(duck_charmer::session_t& session, const std::string& collection, components::storage::document_t doc) {
        init();
        goblin_engineer::send(
            address_book(name_),
            address(),
            "insert",
            session,
            collection,
            std::move(doc));
        wait();
        return  std::get<result_insert_one>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::find(duck_charmer::session_t& session,const std::string& collection, components::storage::document_t condition) -> wrapper_cursor_ptr {
        init();
        goblin_engineer::send(
            address_book(name_),
            address(),
            "find",
            session,
            collection,
            std::move(condition)
            );
        wait();
        auto* result =  std::get<components::cursor::cursor_t*>(intermediate_store_);
        return wrapper_cursor_ptr(new wrapper_cursor(session,result));
    }

    result_size wrapper_dispatcher_t::size(duck_charmer::session_t& session,const  std::string& collection) {
        init();
        goblin_engineer::send(
            address_book(name_),
            address(),
            "size",
            session,
            collection
            );
        wait();
        auto result =  std::get<result_size>(intermediate_store_);
        return result;
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

    auto wrapper_dispatcher_t::insert_finish(duck_charmer::session_t& session,result_insert_one result) -> void {
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    auto wrapper_dispatcher_t::find_finish(duck_charmer::session_t& session,components::cursor::cursor_t* cursor) -> void {
        intermediate_store_ = cursor;
        input_session_ = session;
        notify();
    }

    auto wrapper_dispatcher_t::size_finish(duck_charmer::session_t& session,result_size result) -> void {
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

}