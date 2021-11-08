#include "wrapper_dispatcher.hpp"
#include "wrapper_cursor.hpp"
#include "forward.hpp"
#include "route.hpp"

namespace duck_charmer {

    wrapper_dispatcher_t::wrapper_dispatcher_t(log_t& log,const std::string& name_dispather )
        : manager_t("wrapper_dispatcher")
        , log_(log.clone())
        , name_(name_dispather) {
        add_handler(duck_charmer::manager_database::create_database, &wrapper_dispatcher_t::create_database_finish);
        add_handler(duck_charmer::database::create_collection, &wrapper_dispatcher_t::create_collection_finish);
        add_handler(duck_charmer::collection::insert, &wrapper_dispatcher_t::insert_finish);
        add_handler(duck_charmer::collection::find, &wrapper_dispatcher_t::find_finish);
        add_handler(duck_charmer::collection::size, &wrapper_dispatcher_t::size_finish);
    }

    auto wrapper_dispatcher_t::create_database(duck_charmer::session_t& session, const std::string& name) {
        init();
        goblin_engineer::send(
            address_book(name_),
            address(),
            "create_database",
            session,
            name);
        wait();
        auto result =  std::get<result_size>(intermediate_store_);
        return result;

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
        auto result =  std::get<result_size>(intermediate_store_);
        return result;
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
        auto result =  std::get<result_size>(intermediate_store_);
        return result;
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
        auto result =  std::get<result_size>(intermediate_store_);
        return result;
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

    auto wrapper_dispatcher_t::create_database_finish(duck_charmer::session_t& session) -> void {

    }

    auto wrapper_dispatcher_t::create_collection_finish(duck_charmer::session_t& session) -> void {

    }

    auto wrapper_dispatcher_t::insert_finish(duck_charmer::session_t& session) -> void {

    }

    auto wrapper_dispatcher_t::find_finish(duck_charmer::session_t& session) -> void {

    }

    auto wrapper_dispatcher_t::size_finish(duck_charmer::session_t& session,) -> void {

    }

}