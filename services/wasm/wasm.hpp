#pragma once

#include <filesystem>

#include <components/wasm_runner/wasm.hpp>

#include "core/excutor.hpp"

#include <actor-zeta.hpp>
#include "core/handler_by_id.hpp"

namespace services::wasm {

    enum class route : uint64_t {
        load_code
    };

    inline uint64_t handler_id(route type) {
        return handler_id(group_id_t::wasm, type);
    }


    class wasm_runner_t final : public actor_zeta::basic_async_actor {
    public:
        template<class Manager>
        wasm_runner_t(Manager* env)
            : actor_zeta::basic_async_actor(env, "wasm_runner")
            , wasm_manager_(components::wasm_runner::engine_t::wamr) {
            add_handler(handler_id(route::load_code), &wasm_runner_t::load_code);
        }

    private:
        auto load_code(const std::filesystem::path& path) -> void;

        components::wasm_runner::wasm_manager_t wasm_manager_;
        std::shared_ptr<proxy_wasm::PluginHandleBase> wasm_;
    };

} // namespace services::wasm
