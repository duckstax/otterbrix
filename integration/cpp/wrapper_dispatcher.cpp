#include "wrapper_dispatcher.hpp"
#include "route.hpp"
#include <components/ql/statements/create_collection.hpp>
#include <components/ql/statements/create_database.hpp>
#include <components/ql/statements/delete_many.hpp>
#include <components/ql/statements/delete_one.hpp>
#include <components/ql/statements/drop_collection.hpp>
#include <components/ql/statements/insert_many.hpp>
#include <components/ql/statements/insert_one.hpp>
#include <components/ql/statements/update_many.hpp>
#include <components/ql/statements/update_one.hpp>
#include <components/sql/parser.hpp>
#include <core/system_command.hpp>

using namespace components::cursor;

namespace otterbrix {

    wrapper_dispatcher_t::wrapper_dispatcher_t(std::pmr::memory_resource* mr,
                                               actor_zeta::address_t manager_dispatcher,
                                               log_t& log)
        : actor_zeta::cooperative_supervisor<wrapper_dispatcher_t>(mr, "wrapper_dispatcher")
        , manager_dispatcher_(manager_dispatcher)
        , log_(log.clone())
        , blocker_(mr) {
        add_handler(core::handler_id(core::route::load_finish), &wrapper_dispatcher_t::load_finish);
        add_handler(dispatcher::handler_id(dispatcher::route::execute_ql_finish),
                    &wrapper_dispatcher_t::execute_ql_finish);
        add_handler(collection::handler_id(collection::route::size_finish), &wrapper_dispatcher_t::size_finish);
    }

    wrapper_dispatcher_t::~wrapper_dispatcher_t() { trace(log_, "delete wrapper_dispatcher_t"); }

    auto wrapper_dispatcher_t::load() -> void {
        session_id_t session;
        trace(log_, "wrapper_dispatcher_t::load session: {}", session.data());
        init(session);
        actor_zeta::send(manager_dispatcher_, address(), core::handler_id(core::route::load), session);
        wait(session);
    }

    auto wrapper_dispatcher_t::create_database(const session_id_t& session, const database_name_t& database)
        -> cursor_t_ptr {
        components::ql::create_database_t ql{database};
        return send_ql_new(session, &ql);
    }

    auto wrapper_dispatcher_t::drop_database(const components::session::session_id_t& session,
                                             const database_name_t& database) -> cursor_t_ptr {
        components::ql::drop_database_t ql{database};
        return send_ql_new(session, &ql);
    }

    auto wrapper_dispatcher_t::create_collection(const session_id_t& session,
                                                 const database_name_t& database,
                                                 const collection_name_t& collection) -> cursor_t_ptr {
        components::ql::create_collection_t ql{database, collection};
        return send_ql_new(session, &ql);
    }

    auto wrapper_dispatcher_t::drop_collection(const components::session::session_id_t& session,
                                               const database_name_t& database,
                                               const collection_name_t& collection) -> cursor_t_ptr {
        components::ql::drop_collection_t ql{database, collection};
        return send_ql_new(session, &ql);
    }

    auto wrapper_dispatcher_t::insert_one(const session_id_t& session,
                                          const database_name_t& database,
                                          const collection_name_t& collection,
                                          document_ptr& document) -> cursor_t_ptr {
        trace(log_, "wrapper_dispatcher_t::insert_one session: {}, collection name: {} ", session.data(), collection);
        init(session);
        components::ql::insert_one_t ql{database, collection, document};
        actor_zeta::send(manager_dispatcher_,
                         address(),
                         dispatcher::handler_id(dispatcher::route::execute_ql),
                         session,
                         &ql);
        wait(session);
        return std::move(cursor_store_);
    }

    auto wrapper_dispatcher_t::insert_many(const session_id_t& session,
                                           const database_name_t& database,
                                           const collection_name_t& collection,
                                           std::pmr::vector<document_ptr>& documents) -> cursor_t_ptr {
        trace(log_, "wrapper_dispatcher_t::insert_many session: {}, collection name: {} ", session.data(), collection);
        init(session);
        components::ql::insert_many_t ql{database, collection, documents};
        actor_zeta::send(manager_dispatcher_,
                         address(),
                         dispatcher::handler_id(dispatcher::route::execute_ql),
                         session,
                         &ql);
        wait(session);
        return std::move(cursor_store_);
    }

    auto wrapper_dispatcher_t::find(const session_id_t& session, components::ql::aggregate_statement_raw_ptr condition)
        -> cursor_t_ptr {
        trace(log_,
              "wrapper_dispatcher_t::find session: {}, database: {} collection: {} ",
              session.data(),
              condition->database_,
              condition->collection_);
        std::unique_ptr<components::ql::aggregate_statement> ql(condition);
        return send_ql_new(session, ql.get());
    }

    auto wrapper_dispatcher_t::find_one(const components::session::session_id_t& session,
                                        components::ql::aggregate_statement_raw_ptr condition) -> cursor_t_ptr {
        trace(log_,
              "wrapper_dispatcher_t::find_one session: {}, database: {} collection: {} ",
              session.data(),
              condition->database_,
              condition->collection_);
        std::unique_ptr<components::ql::aggregate_statement> ql(condition);
        return send_ql_new(session, ql.get());
    }

    auto wrapper_dispatcher_t::delete_one(const components::session::session_id_t& session,
                                          components::ql::aggregate_statement_raw_ptr condition) -> cursor_t_ptr {
        trace(log_,
              "wrapper_dispatcher_t::delete_one session: {}, database: {} collection: {} ",
              session.data(),
              condition->database_,
              condition->collection_);
        init(session);
        components::ql::delete_one_t ql{condition};
        std::unique_ptr<components::ql::ql_statement_t> _(condition);
        actor_zeta::send(manager_dispatcher_,
                         address(),
                         dispatcher::handler_id(dispatcher::route::execute_ql),
                         session,
                         &ql);
        wait(session);
        return std::move(cursor_store_);
    }

    auto wrapper_dispatcher_t::delete_many(const components::session::session_id_t& session,
                                           components::ql::aggregate_statement_raw_ptr condition) -> cursor_t_ptr {
        trace(log_,
              "wrapper_dispatcher_t::delete_many session: {}, database: {} collection: {} ",
              session.data(),
              condition->database_,
              condition->collection_);
        init(session);
        components::ql::delete_many_t ql{condition};
        std::unique_ptr<components::ql::ql_statement_t> _(condition);
        actor_zeta::send(manager_dispatcher_,
                         address(),
                         dispatcher::handler_id(dispatcher::route::execute_ql),
                         session,
                         &ql);
        wait(session);
        return std::move(cursor_store_);
    }

    auto wrapper_dispatcher_t::update_one(const components::session::session_id_t& session,
                                          components::ql::aggregate_statement_raw_ptr condition,
                                          document_ptr update,
                                          bool upsert) -> cursor_t_ptr {
        trace(log_,
              "wrapper_dispatcher_t::update_one session: {}, database: {} collection: {} ",
              session.data(),
              condition->database_,
              condition->collection_);
        init(session);
        components::ql::update_one_t ql{condition, update, upsert};
        std::unique_ptr<components::ql::ql_statement_t> _(condition);
        actor_zeta::send(manager_dispatcher_,
                         address(),
                         dispatcher::handler_id(dispatcher::route::execute_ql),
                         session,
                         &ql);
        wait(session);
        return std::move(cursor_store_);
    }

    auto wrapper_dispatcher_t::update_many(const components::session::session_id_t& session,
                                           components::ql::aggregate_statement_raw_ptr condition,
                                           document_ptr update,
                                           bool upsert) -> cursor_t_ptr {
        trace(log_,
              "wrapper_dispatcher_t::update_many session: {}, database: {} collection: {} ",
              session.data(),
              condition->database_,
              condition->collection_);
        init(session);
        components::ql::update_many_t ql{condition, update, upsert};
        std::unique_ptr<components::ql::ql_statement_t> _(condition);
        actor_zeta::send(manager_dispatcher_,
                         address(),
                         dispatcher::handler_id(dispatcher::route::execute_ql),
                         session,
                         &ql);
        wait(session);
        return std::move(cursor_store_);
    }

    auto wrapper_dispatcher_t::size(const session_id_t& session,
                                    const database_name_t& database,
                                    const collection_name_t& collection) -> size_t {
        trace(log_, "wrapper_dispatcher_t::size session: {}, collection name : {} ", session.data(), collection);
        init(session);
        actor_zeta::send(manager_dispatcher_,
                         address(),
                         collection::handler_id(collection::route::size),
                         session,
                         database,
                         collection);
        wait(session);
        return std::move(size_store_);
    }

    auto wrapper_dispatcher_t::create_index(const session_id_t& session, components::ql::create_index_t* ql)
        -> components::cursor::cursor_t_ptr {
        trace(log_, "wrapper_dispatcher_t::create_index session: {}, index: {}", session.data(), ql->name());
        return send_ql_new(session, ql);
    }

    auto wrapper_dispatcher_t::drop_index(const session_id_t& session, components::ql::drop_index_t* ql)
        -> components::cursor::cursor_t_ptr {
        trace(log_, "wrapper_dispatcher_t::create_index session: {}, index: {}", session.data(), ql->name());
        return send_ql_new(session, ql);
    }

    auto wrapper_dispatcher_t::execute_ql(const session_id_t& session, components::ql::variant_statement_t& query)
        -> cursor_t_ptr {
        using namespace components::ql;

        trace(log_, "wrapper_dispatcher_t::execute session: {}", session.data());

        return std::visit(
            [&](auto& ql) {
                using type = std::decay_t<decltype(ql)>;
                if constexpr (std::is_same_v<type, ql_statement_t*>) {
                    return send_ql_new(session, ql);
                } else {
                    return send_ql_new(session, &ql);
                }
            },
            query);
    }

    cursor_t_ptr wrapper_dispatcher_t::execute_sql(const components::session::session_id_t& session,
                                                   const std::string& query) {
        trace(log_, "wrapper_dispatcher_t::execute sql session: {}", session.data());
        auto parse_result = components::sql::parse(resource(), query);
        if (parse_result.error) {
            error(log_, parse_result.error.what());
            return make_cursor(resource(), error_code_t::sql_parse_error, parse_result.error.what().data());
        } else {
            return execute_ql(session, parse_result.ql);
        }
        return make_cursor(resource(), error_code_t::sql_parse_error, "not valid sql");
    }

    auto wrapper_dispatcher_t::scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* {
        assert("wrapper_dispatcher_t::executor_impl");
        return nullptr;
    }

    auto wrapper_dispatcher_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
        std::unique_lock<spin_lock> _(input_mtx_);
        auto tmp = std::move(msg);
        trace(log_, "wrapper_dispatcher_t::enqueue_base msg type: {}", tmp->command().integer_value());
        set_current_message(std::move(tmp));
        execute(this, current_message());
    }

    auto wrapper_dispatcher_t::load_finish(const session_id_t& session) -> void {
        trace(log_, "wrapper_dispatcher_t::load_finish");
        notify(session);
    }

    void wrapper_dispatcher_t::execute_ql_finish(const session_id_t& session, cursor_t_ptr cursor) {
        trace(log_, "wrapper_dispatcher_t::execute_ql_finish session: {} {}", session.data(), cursor->is_success());
        cursor_store_ = cursor;
        notify(session);
    }

    auto wrapper_dispatcher_t::size_finish(const session_id_t& session, size_t size) -> void {
        trace(log_, "wrapper_dispatcher_t::size_finish session: {} {}", session.data(), size);
        size_store_ = size;
        notify(session);
    }

    void wrapper_dispatcher_t::init(const session_id_t& session) { blocker_.set_value(session, false); }

    void wrapper_dispatcher_t::wait(const session_id_t& session) {
        std::unique_lock<std::mutex> lk(output_mtx_);
        cv_.wait(lk, [this, &session]() { return blocker_.value(session); });
        blocker_.remove_session(session);
    }

    void wrapper_dispatcher_t::notify(const session_id_t& session) {
        blocker_.set_value(session, true);
        cv_.notify_all();
    }

    cursor_t_ptr wrapper_dispatcher_t::send_ql_new(const session_id_t& session, components::ql::ql_statement_t* ql) {
        trace(log_, "wrapper_dispatcher_t::send_ql session: {}, {} ", session.data(), ql->to_string());
        init(session);
        actor_zeta::send(manager_dispatcher_,
                         address(),
                         dispatcher::handler_id(dispatcher::route::execute_ql),
                         session,
                         ql);
        wait(session);
        if (cursor_store_->is_error()) {
            //todo: handling error
            std::cerr << cursor_store_->get_error().what << std::endl;
        }

        return std::move(cursor_store_);
    }

    template<typename Tql>
    auto wrapper_dispatcher_t::send_ql(const session_id_t& session, Tql& ql, std::string_view title, uint64_t handle)
        -> cursor_t_ptr {
        trace(log_,
              "wrapper_dispatcher_t::{} session: {}, database: {} collection: {} ",
              title,
              session.data(),
              ql.database_,
              ql.collection_);
        init(session);
        actor_zeta::send(manager_dispatcher_, address(), handle, session, &ql);
        wait(session);
        return std::move(cursor_store_);
    }

} // namespace otterbrix
