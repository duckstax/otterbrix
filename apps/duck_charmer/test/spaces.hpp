#pragma once

#include <goblin-engineer/core.hpp>
#include <components/log/log.hpp>
#include <components/protocol/base.hpp>

namespace test {

    using manager_t = goblin_engineer::basic_manager_service_t<goblin_engineer::base_policy_light>;

    class spaces_t final : public manager_t {
    public:
        spaces_t(spaces_t& other) = delete;
        void operator=(const spaces_t&) = delete;

        static spaces_t &get();
        log_t& log();

        void create_database(const database_name_t &database_name);
        void create_collection(const database_name_t &database_name, const collection_name_t &collection_name);
        void drop_collection(const database_name_t &database, const collection_name_t &collection);

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
        spaces_t();

        log_t log_;
        goblin_engineer::supervisor manager_database_;
        goblin_engineer::supervisor manager_dispatcher_;
        goblin_engineer::supervisor manager_wal_;
        const std::string name_;
        std::atomic_int i = 0;
        std::mutex output_mtx_;
        spin_lock input_mtx_;
        std::condition_variable cv_;
        duck_charmer::session_id_t input_session_;
        duck_charmer::session_id_t output_session_;
        std::variant<
            result_insert_one,
            result_insert_many,
            components::cursor::cursor_t*,
            result_find_one,
            result_size,
            result_delete,
            result_update,
            result_drop_collection,
            services::storage::database_create_result,
            services::storage::collection_create_result>
            intermediate_store_;

        void init();
        void wait();
        void notify();
    };

} //namespace test
