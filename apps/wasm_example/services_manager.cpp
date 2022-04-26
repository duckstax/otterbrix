#include "services_manager.hpp"

using namespace std;
using namespace services::wasm;

example_wasm_t::example_wasm_t()
    : goblin_engineer::abstract_manager_service("example_wasm")
    , coordinator_(make_unique<actor_zeta::executor_t<actor_zeta::work_sharing>>(1, 1000))
    , system_{"start"} {
    add_handler("start", &example_wasm_t::start);
    coordinator_->start();
}

auto example_wasm_t::executor_impl() noexcept -> actor_zeta::executor::abstract_executor* {
    return coordinator_.get();
}

auto example_wasm_t::enqueue_base(actor_zeta::message_ptr msg, actor_zeta::execution_device*) -> void {
    auto it = system_.find(msg->command());

    if (it != system_.end()) {
        set_current_message(move(msg));
        execute();
    }
}

auto example_wasm_t::start(const filesystem::path& wasm_path) -> void {
    auto wasm = address_book("wasm_runner");

    if (!wasm) {
        wasm = spawn_actor<wasm_runner_t>();
    }

    actor_zeta::send(wasm, address(), "load_code", wasm_path);
}
