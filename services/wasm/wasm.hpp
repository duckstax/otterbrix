#pragma once

#include <boost/filesystem.hpp>

#include <components/wasm_runner/wasm.hpp>

#include "core/excutor.hpp"

#include <actor-zeta.hpp>
#include "core/handler_by_id.hpp"
namespace services::wasm {

    enum class route : uint64_t {
        create
    };

    inline uint64_t handler_id(route type) {
        return handler_id(group_id_t::wasm, type);
    }

    class manager_wasm_runner_t final : public actor_zeta::cooperative_supervisor<manager_wasm_runner_t> {
    public:
        manager_wasm_runner_t(actor_zeta::detail::pmr::memory_resource*mr,log_t& log, size_t num_workers, size_t max_throughput)
            : actor_zeta::cooperative_supervisor<manager_wasm_runner_t>(mr, "manager_wasm_runner")
            , log_(log.clone())
            , e_(new actor_zeta::shared_work(num_workers, max_throughput), actor_zeta::detail::thread_pool_deleter()) {

            e_->start();
        }

        auto scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* {}
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void override{}
        ~manager_wasm_runner_t(){}
    private:
        log_t log_;
        actor_zeta::scheduler_ptr e_;

    };

    class wasm_runner_t final : public actor_zeta::basic_async_actor {
    public:
        wasm_runner_t(manager_wasm_runner_t* );

    private:
        auto load_code(const boost::filesystem::path& path) -> void;

        components::wasm_runner::wasm_manager_t wasm_manager_;
        std::shared_ptr<proxy_wasm::PluginHandleBase> wasm_;
    };

} // namespace services::wasm
