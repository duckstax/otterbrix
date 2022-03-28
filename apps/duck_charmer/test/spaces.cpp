#include "spaces.hpp"
#include <components/session/session.hpp>
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

    void spaces_t::create_database(const database_name_t &database_name) {
        auto session = components::session::session_id_t();
        trace(log_, "spaces::create_database session: {}, database name : {} ", session.data(), database_name);
        goblin_engineer::send(
            manager_dispatcher_,
            goblin_engineer::address_t::empty_address(),
            manager_database::create_database,
            session,
            database_name);
    }

    void spaces_t::create_collection(const database_name_t &database_name, const collection_name_t &collection_name) {
//        log_.trace("wrapper_dispatcher_t::create_collection session: {}, database name : {} , collection name : {} ", session.data(), database_name,collection_name);
//        log_.trace("type address : {}",  address_book("manager_dispatcher").type());
//        init();
//        goblin_engineer::send(
//            address_book("manager_dispatcher"),
//            address(),
//            "create_collection",
//            session,
//            database_name,
//            collection_name);
//        wait();
//        auto result =  std::get<services::storage::collection_create_result>(intermediate_store_);
//        return wrapper_collection_ptr(new wrapper_collection(collection_name,database_name,this,log_));
    }

    void spaces_t::drop_collection(const database_name_t &database_name, const collection_name_t &collection_name) {
//        log_.trace("wrapper_dispatcher_t::drop_collection session: {}, database name: {}, collection name: {} ", session.data(), database, collection);
//        init();
//        goblin_engineer::send(
//            address_book("manager_dispatcher"),
//            address(),
//            database::drop_collection,
//            session,
//            database,
//            collection
//        );
//        wait();
//        return std::get<result_drop_collection>(intermediate_store_);
    }

    auto spaces_t::add_actor_impl(goblin_engineer::actor) -> void override{
        throw std::runtime_error("wrapper_dispatcher_t::add_actor_impl");
    }
    auto spaces_t::add_supervisor_impl(goblin_engineer::supervisor) -> void override {
        throw std::runtime_error("wrapper_dispatcher_t::add_supervisor_impl");
    }

    auto spaces_t::executor_impl() noexcept -> goblin_engineer::abstract_executor*  {
        throw std::runtime_error("wrapper_dispatcher_t::executor_impl");
    }

    auto spaces_t::enqueue_base(actor_zeta::message_ptr msg, actor_zeta::execution_device*) -> void {
        std::unique_lock<spin_lock> _(input_mtx_);
        auto tmp = std::move(msg);
        log_.trace("wrapper_dispatcher_t::enqueue_base msg type: {}",tmp->command());
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

        trace(log_, "spaces finish");
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
