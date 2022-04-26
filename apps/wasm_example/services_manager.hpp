#pragma once

#include <services/wasm/wasm.hpp>

class example_wasm_t final : public goblin_engineer::abstract_manager_service {
public:
    example_wasm_t();

    auto executor_impl() noexcept -> actor_zeta::executor::abstract_executor* override;

    auto enqueue_base(actor_zeta::message_ptr msg, actor_zeta::execution_device*) -> void override;

private:
    auto start(const std::filesystem::path& wasm_path) -> void;

    std::unique_ptr<actor_zeta::executor::abstract_executor> coordinator_;
    std::unordered_set<actor_zeta::detail::string_view> system_;
};
