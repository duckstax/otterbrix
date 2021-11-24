#pragma once
#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>

#include <pybind11/pybind11.h>

#include <components/log/log.hpp>
#include <goblin-engineer/core.hpp>

#include "cursor/cursor.hpp"
#include "forward.hpp"
#include <components/document/document.hpp>
#include "services/storage/result.hpp"
#include "wrapper_cursor.hpp"
#include "services/storage/result_database.hpp"

class spin_lock final {
public:
    spin_lock() = default;
    spin_lock(const spin_lock&) = delete;
    spin_lock(spin_lock&&) = default;
    spin_lock& operator=(const spin_lock&) = delete;
    spin_lock& operator=(spin_lock&&) = default;
    void lock() {
        while (_lock.test_and_set(std::memory_order_acquire)) continue;
    }
    void unlock() {
        _lock.clear(std::memory_order_release);
    }

private:
    std::atomic_flag _lock = ATOMIC_FLAG_INIT;
};

namespace duck_charmer {

    using manager_t = goblin_engineer::basic_manager_service_t<goblin_engineer::base_policy_light>;

    class PYBIND11_EXPORT wrapper_dispatcher_t final : public manager_t {
    public:
        /// blocking method
        wrapper_dispatcher_t(log_t& log,const std::string& name_dispather );
        auto create_database(duck_charmer::session_t& session, const std::string& name) -> wrapper_database_ptr ;
        auto create_collection(duck_charmer::session_t& session,const std::string& database_name, const std::string& collection_name) -> wrapper_collection_ptr;
        result_drop_collection drop_collection(duck_charmer::session_t& session, const std::string& database, const std::string& collection);
        auto insert_one(duck_charmer::session_t& session, const std::string& database, const std::string& collection, components::document::document_t &document) -> result_insert_one&;
        auto insert_many(duck_charmer::session_t& session, const std::string& database, const std::string& collection, std::list<components::document::document_t> &documents) -> result_insert_many&;
        auto find(duck_charmer::session_t& session, const std::string& database, const std::string& collection, components::document::document_t condition) -> wrapper_cursor_ptr;
        auto find_one(duck_charmer::session_t& session, const std::string& database, const std::string& collection, components::document::document_t condition) -> result_find_one&;
        result_size size(duck_charmer::session_t& session, const std::string& database, const std::string& collection);

    protected:
        auto add_actor_impl(goblin_engineer::actor) -> void override{
            throw std::runtime_error("wrapper_dispatcher_t::add_actor_impl");
        }
        auto add_supervisor_impl(goblin_engineer::supervisor) -> void override {
            throw std::runtime_error("wrapper_dispatcher_t::add_supervisor_impl");
        }

        auto executor_impl() noexcept -> goblin_engineer::abstract_executor*  {
            throw std::runtime_error("wrapper_dispatcher_t::executor_impl");
        }

        auto enqueue_base(actor_zeta::message_ptr msg, actor_zeta::execution_device*) -> void {
            std::unique_lock<spin_lock> _(input_mtx_);
            auto tmp = std::move(msg);
            log_.trace("wrapper_dispatcher_t::enqueue_base msg type: {}",tmp->command());
            set_current_message(std::move(tmp));
            execute();
        }

    private:
        /// async method
        auto create_database_finish(duck_charmer::session_t&,services::storage::database_create_result) -> void;
        auto create_collection_finish(duck_charmer::session_t& session,services::storage::collection_create_result) -> void;
        auto drop_collection_finish(duck_charmer::session_t& session,result_drop_collection result) -> void;
        auto insert_one_finish(duck_charmer::session_t& session,result_insert_one result) -> void;
        auto insert_many_finish(duck_charmer::session_t& session,result_insert_many result) -> void;
        auto find_finish(duck_charmer::session_t& session,components::cursor::cursor_t*) -> void;
        auto find_one_finish(duck_charmer::session_t& session,result_find_one result) -> void;
        auto size_finish(duck_charmer::session_t& session,result_size result) -> void;

        void init() {
            i = 0;
        }

        void wait() {
            std::unique_lock<std::mutex> lk(output_mtx_);
            cv_.wait(lk, [this]() { return i == 1; });
        }

        void notify() {
            i = 1;
            cv_.notify_all();
        }

        log_t log_;
        const std::string name_;
        std::atomic_int i = 0;
        std::mutex output_mtx_;
        spin_lock input_mtx_;
        std::condition_variable cv_;
        duck_charmer::session_t input_session_;
        duck_charmer::session_t output_session_;
        std::variant<
            result_insert_one,
            result_insert_many,
            components::cursor::cursor_t*,
            result_find_one,
            result_size,
            result_drop_collection,
            services::storage::database_create_result,
            services::storage::collection_create_result>
            intermediate_store_;
    };
}