#include "wrapper_dispatcher.hpp"
#include "route.hpp"
#include <core/system_command.hpp>
#include <components/ql/statements/create_collection.hpp>
#include <components/ql/statements/create_database.hpp>
#include <components/ql/statements/drop_collection.hpp>
#include <components/ql/statements/delete_many.hpp>
#include <components/ql/statements/delete_one.hpp>
#include <components/ql/statements/insert_many.hpp>
#include <components/ql/statements/insert_one.hpp>
#include <components/ql/statements/update_many.hpp>
#include <components/ql/statements/update_one.hpp>
#include <components/sql/parser.hpp>

using namespace components::result;

namespace duck_charmer {

    wrapper_dispatcher_t::wrapper_dispatcher_t(std::pmr::memory_resource* mr, actor_zeta::address_t manager_dispatcher, log_t& log)
        : actor_zeta::cooperative_supervisor<wrapper_dispatcher_t>(mr)
        , load_finish_(actor_zeta::make_behavior(resource(), core::handler_id(core::route::load_finish), this, &wrapper_dispatcher_t::load_finish))
        , execute_ql_finish_(actor_zeta::make_behavior(resource(), dispatcher::handler_id(dispatcher::route::execute_ql_finish), this, &wrapper_dispatcher_t::execute_ql_finish))
        , insert_finish_(actor_zeta::make_behavior(resource(), collection::handler_id(collection::route::insert_finish), this, &wrapper_dispatcher_t::insert_finish))
        , delete_finish_(actor_zeta::make_behavior(resource(), collection::handler_id(collection::route::delete_finish), this, &wrapper_dispatcher_t::delete_finish))
        , update_finish_(actor_zeta::make_behavior(resource(), collection::handler_id(collection::route::update_finish), this, &wrapper_dispatcher_t::update_finish))
        , size_finish_(actor_zeta::make_behavior(resource(), collection::handler_id(collection::route::size_finish), this, &wrapper_dispatcher_t::size_finish))
        , create_index_finish_(actor_zeta::make_behavior(resource(), collection::handler_id(collection::route::create_index_finish), this, &wrapper_dispatcher_t::create_index_finish))
        , drop_index_finish_(actor_zeta::make_behavior(resource(), collection::handler_id(collection::route::drop_index_finish), this, &wrapper_dispatcher_t::drop_index_finish))
        , manager_dispatcher_(manager_dispatcher)
        , log_(log.clone()) {
    }

    actor_zeta::behavior_t wrapper_dispatcher_t::behavior() {
        return actor_zeta::make_behavior(
            resource(),
            [this](actor_zeta::message* msg) -> void {
                switch (msg->command()) {

                }
            }
        );
    }

    auto wrapper_dispatcher_t::make_type() const noexcept -> const char* const{
        return "wrapper_dispatcher";
    }

    wrapper_dispatcher_t::~wrapper_dispatcher_t() {
        trace(log_, "delete wrapper_dispatcher_t");
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

    auto wrapper_dispatcher_t::create_database(session_id_t &session, const database_name_t &database) -> result_t {
        components::ql::create_database_t ql{database};
        return send_ql_new(session, &ql);
    }

    auto wrapper_dispatcher_t::drop_database(components::session::session_id_t& session, const database_name_t& database) -> result_t {
        components::ql::drop_database_t ql{database};
        return send_ql_new(session, &ql);
    }

    auto wrapper_dispatcher_t::create_collection(session_id_t &session, const database_name_t &database, const collection_name_t &collection) -> result_t {
        components::ql::create_collection_t ql{database, collection};
        return send_ql_new(session, &ql);
    }

    auto wrapper_dispatcher_t::drop_collection(components::session::session_id_t &session, const database_name_t &database, const collection_name_t &collection) -> result_t {
        components::ql::drop_collection_t ql{database, collection};
        return send_ql_new(session, &ql);
    }

    auto wrapper_dispatcher_t::insert_one(session_id_t &session, const database_name_t &database, const collection_name_t &collection, document_ptr &document) -> result_insert & {
        trace(log_, "wrapper_dispatcher_t::insert_one session: {}, collection name: {} ", session.data(), collection);
        init();
        components::ql::insert_one_t ql{database, collection, document};
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            collection::handler_id(collection::route::insert_documents),
            session,
            &ql);
        wait();
        return std::get<result_insert>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::insert_many(session_id_t &session, const database_name_t &database, const collection_name_t &collection, std::pmr::vector<document_ptr> &documents) -> result_insert & {
        trace(log_, "wrapper_dispatcher_t::insert_many session: {}, collection name: {} ", session.data(), collection);
        init();
        components::ql::insert_many_t ql{database, collection, documents};
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            collection::handler_id(collection::route::insert_documents),
            session,
            &ql);
        wait();
        return std::get<result_insert>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::find(session_id_t &session, components::ql::aggregate_statement_raw_ptr condition) -> result_t {
        trace(log_, "wrapper_dispatcher_t::find session: {}, database: {} collection: {} ", session.data(), condition->database_, condition->collection_);
        std::unique_ptr<components::ql::aggregate_statement> ql(condition);
        return send_ql_new(session, ql.get());
    }

    auto wrapper_dispatcher_t::find_one(components::session::session_id_t &session, components::ql::aggregate_statement_raw_ptr condition) -> result_t {
        trace(log_, "wrapper_dispatcher_t::find_one session: {}, database: {} collection: {} ", session.data(), condition->database_, condition->collection_);
        std::unique_ptr<components::ql::aggregate_statement> ql(condition);
        return send_ql_new(session, ql.get());
    }

    auto wrapper_dispatcher_t::delete_one(components::session::session_id_t &session, components::ql::aggregate_statement_raw_ptr condition) -> result_delete & {
        trace(log_, "wrapper_dispatcher_t::delete_one session: {}, database: {} collection: {} ", session.data(), condition->database_, condition->collection_);
        init();
        components::ql::delete_one_t ql{condition};
        std::unique_ptr<components::ql::ql_statement_t> _(condition);
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            collection::handler_id(collection::route::delete_documents),
            session,
            &ql);
        wait();
        return std::get<result_delete>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::delete_many(components::session::session_id_t &session, components::ql::aggregate_statement_raw_ptr condition) -> result_delete &{
        trace(log_, "wrapper_dispatcher_t::delete_many session: {}, database: {} collection: {} ", session.data(), condition->database_, condition->collection_);
        init();
        components::ql::delete_many_t ql{condition};
        std::unique_ptr<components::ql::ql_statement_t> _(condition);
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            collection::handler_id(collection::route::delete_documents),
            session,
            &ql);
        wait();
        return std::get<result_delete>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::update_one(components::session::session_id_t &session, components::ql::aggregate_statement_raw_ptr condition, document_ptr update, bool upsert) -> result_update & {
        trace(log_, "wrapper_dispatcher_t::update_one session: {}, database: {} collection: {} ", session.data(), condition->database_, condition->collection_);
        init();
        components::ql::update_one_t ql{condition, update, upsert};
        std::unique_ptr<components::ql::ql_statement_t> _(condition);
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            collection::handler_id(collection::route::update_documents),
            session,
            &ql);
        wait();
        return std::get<result_update>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::update_many(components::session::session_id_t &session, components::ql::aggregate_statement_raw_ptr condition, document_ptr update, bool upsert) -> result_update & {
        trace(log_, "wrapper_dispatcher_t::update_many session: {}, database: {} collection: {} ", session.data(), condition->database_, condition->collection_);
        init();
        components::ql::update_many_t ql{condition, update, upsert};
        std::unique_ptr<components::ql::ql_statement_t> _(condition);
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            collection::handler_id(collection::route::update_documents),
            session,
            &ql);
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
        trace(log_, "wrapper_dispatcher_t::create_index session: {}, index: {}", session.data(), index.name());
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

    auto wrapper_dispatcher_t::drop_index(session_id_t &session, components::ql::drop_index_t drop_index) -> result_drop_index {
        trace(log_, "wrapper_dispatcher_t::drop_index session: {}, index: {}", session.data(), drop_index.name());
        init();
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            collection::handler_id(collection::route::drop_index),
            session,
            std::move(drop_index));
        wait();
        return std::get<result_drop_index>(intermediate_store_);
    }

    auto wrapper_dispatcher_t::execute_ql(session_id_t &session, components::ql::variant_statement_t &query) -> result_t {
        using namespace components::ql;

        trace(log_, "wrapper_dispatcher_t::execute session: {}", session.data());

        return std::visit([&](auto& ql) {
            using type = std::decay_t<decltype(ql)>;
            if constexpr (std::is_same_v<type, insert_many_t>) {
                return send_ql<result_insert>(session, ql, "insert", collection::handler_id(collection::route::insert_documents));
            } else if constexpr (std::is_same_v<type, delete_many_t>) {
                return send_ql<result_delete>(session, ql, "delete", collection::handler_id(collection::route::delete_documents));
            } else if constexpr (std::is_same_v<type, update_many_t>) {
                return send_ql<result_update>(session, ql, "update", collection::handler_id(collection::route::update_documents));
            } else if constexpr (std::is_same_v<type, ql_statement_t*>) {
                return send_ql_new(session, ql);
            } else {
                return send_ql_new(session, &ql);
            }
        }, query);
    }

    result_t wrapper_dispatcher_t::execute_sql(components::session::session_id_t& session, const std::string& query) {
        trace(log_, "wrapper_dispatcher_t::execute sql session: {}", session.data());
        auto parse_result = components::sql::parse(resource(), query);
        if (parse_result.error) {
            error(log_, parse_result.error.what());
            return make_error(error_code_t::sql_parse_error, parse_result.error.what().data());
        } else {
            return execute_ql(session, parse_result.ql);
        }
        return make_error(error_code_t::sql_parse_error, "not valid sql");
    }

    auto wrapper_dispatcher_t::make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t* {
        assert("wrapper_dispatcher_t::executor_impl");
        return nullptr;
    }

    auto wrapper_dispatcher_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
        std::unique_lock<spin_lock> _(input_mtx_);
        auto tmp = std::move(msg);
        trace(log_, "wrapper_dispatcher_t::enqueue_base msg type: {}", tmp->command().integer_value());
        set_current_message(std::move(tmp));
        behavior()(current_message());
    }

    auto wrapper_dispatcher_t::load_finish() -> void {
        trace(log_, "wrapper_dispatcher_t::load_finish");
        notify();
    }

    void wrapper_dispatcher_t::execute_ql_finish(session_id_t& session, const result_t& result) {
        trace(log_, "wrapper_dispatcher_t::execute_ql_finish session: {} {}", session.data(), result.is_success());
        intermediate_store_ = result;
        input_session_ = session;
        notify();
    }

    void wrapper_dispatcher_t::insert_finish(session_id_t &session, result_insert result) {
        trace(log_, "wrapper_dispatcher_t::insert_finish session: {}, result: {} inserted", session.data(), result.inserted_ids().size());
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

    auto wrapper_dispatcher_t::drop_index_finish(session_id_t &session, result_drop_index result) -> void {
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

    result_t wrapper_dispatcher_t::send_ql_new(session_id_t& session, components::ql::ql_statement_t* ql) {
        trace(log_, "wrapper_dispatcher_t::send_ql session: {}, {} ", session.data(), ql->to_string());
        init();
        actor_zeta::send(
            manager_dispatcher_,
            address(),
            dispatcher::handler_id(dispatcher::route::execute_ql),
            session,
            ql);
        wait();

        auto& result = std::get<result_t>(intermediate_store_);
        if (result.is_error()) {
            //todo: handling error
            std::cerr << result.error_what() << std::endl;
        }

        return result;
    }

    template <typename Tres, typename Tql>
    auto wrapper_dispatcher_t::send_ql(session_id_t &session, Tql& ql, std::string_view title, uint64_t handle) -> result_t {
        trace(log_, "wrapper_dispatcher_t::{} session: {}, database: {} collection: {} ",
              title, session.data(), ql.database_, ql.collection_);
        init();
        actor_zeta::send(
                    manager_dispatcher_,
                    address(),
                    handle,
                    session,
                    &ql);
        wait();
        return make_result(std::get<Tres>(intermediate_store_));
    }

} // namespace python
