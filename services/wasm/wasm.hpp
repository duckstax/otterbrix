#pragma once

#include <filesystem>

#include <goblin-engineer/core.hpp>

#include <components/wasm_runner/wasm.hpp>

namespace services::wasm {

    class wasm_runner_t final : public goblin_engineer::abstract_service {
    public:
        wasm_runner_t(actor_zeta::base::supervisor_abstract* env);

    private:
        auto load_code(const std::filesystem::path& path) -> void;

        components::wasm_runner::wasm_manager_t wasm_manager_;
        std::shared_ptr<proxy_wasm::PluginHandleBase> wasm_;
    };

} // namespace services::wasm
