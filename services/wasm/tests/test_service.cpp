#include <filesystem>

#include <catch2/catch.hpp>

#include <components/log/log.hpp>

#include "wasm.hpp"

using namespace std;
using namespace services::wasm;

class test_wasm_t final : public goblin_engineer::abstract_manager_service {
public:
    test_wasm_t();

    auto executor_impl() noexcept -> actor_zeta::executor::abstract_executor* override;

    auto enqueue_base(actor_zeta::message_ptr msg, actor_zeta::execution_device*) -> void override;

private:
    auto start(const boost::filesystem::path& wasm_path) -> void;

    unique_ptr<actor_zeta::executor::abstract_executor> coordinator_;
    unordered_set<actor_zeta::detail::string_view> system_;
};

test_wasm_t::test_wasm_t()
    : goblin_engineer::abstract_manager_service("example_wasm")
    , coordinator_(make_unique<actor_zeta::executor_t<actor_zeta::work_sharing>>(1, 1000))
    , system_{"start"} {
    add_handler("start", &test_wasm_t::start);
    coordinator_->start();
}

auto test_wasm_t::executor_impl() noexcept -> actor_zeta::executor::abstract_executor* {
    return coordinator_.get();
}

auto test_wasm_t::enqueue_base(actor_zeta::message_ptr msg, actor_zeta::execution_device*) -> void {
    auto it = system_.find(msg->command());

    if (it != system_.end()) {
        set_current_message(move(msg));
        execute();
    }
}

auto test_wasm_t::start(const boost::filesystem::path& wasm_path) -> void {
    auto wasm = address_book("wasm_runner");

    if (!wasm) {
        wasm = spawn_actor<wasm_runner_t>();
    }

    actor_zeta::send(wasm, address(), "load_code", wasm_path);
}

TEST_CASE("wasm_t", "[API]") {
    string log_dir(".");
    auto log = initialization_logger("wasm", log_dir);
    auto services_manager = goblin_engineer::make_manager_service<test_wasm_t>();

    actor_zeta::send(services_manager, actor_zeta::address_t::empty_address(), "start", boost::filesystem::path("tests") / "log_wasm.wasm");

    // Temporary hack
    std::this_thread::sleep_for(std::chrono::seconds(1));
}
