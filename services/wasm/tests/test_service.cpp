#include <catch2/catch.hpp>

#include <components/log/log.hpp>
#include <core/excutor.hpp>

#include "wasm.hpp"

using namespace std;
using namespace services;

enum class route : uint64_t { start };

inline uint64_t test_handler_id(route type) { return 100 * 3773 + static_cast<uint64_t>(type); }

class test_wasm_t final : public actor_zeta::cooperative_supervisor<test_wasm_t> {
public:
    test_wasm_t(actor_zeta::detail::pmr::memory_resource*);

    auto scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* override;
    auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void override;

private:
    auto start(const std::filesystem::path& wasm_path) -> void;
    actor_zeta::scheduler_ptr coordinator_;
    std::vector<actor_zeta::actor> actors_;
};

test_wasm_t::test_wasm_t(actor_zeta::detail::pmr::memory_resource* resource)
    : actor_zeta::cooperative_supervisor<test_wasm_t>(resource, "example_wasm")
    , coordinator_(new actor_zeta::shared_work(1, 1000), actor_zeta::detail::thread_pool_deleter()) {
    add_handler(test_handler_id(route::start), &test_wasm_t::start);
    coordinator_->start();
}

auto test_wasm_t::scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* { return coordinator_.get(); }

auto test_wasm_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
    set_current_message(move(msg));
    execute(this, current_message());
}

auto test_wasm_t::start(const std::filesystem::path& wasm_path) -> void {
    auto wasm = spawn_actor<wasm::wasm_runner_t>([this](wasm::wasm_runner_t* ptr) { actors_.emplace_back(ptr); });

    actor_zeta::send(wasm, address(), wasm::handler_id(wasm::route::load_code), wasm_path);
}

TEST_CASE("wasm_runner_t load and execute", "[API]") {
    string log_dir(".");
    auto log = initialization_logger("wasm", log_dir);
    actor_zeta::detail::pmr::memory_resource* resource = actor_zeta::detail::pmr::get_default_resource();
    auto services_manager = actor_zeta::spawn_supervisor<test_wasm_t>(resource);

    actor_zeta::send(services_manager,
                     actor_zeta::address_t::empty_address(),
                     test_handler_id(route::start),
                     std::filesystem::path("tests") / "log_wasm.wasm");

    // Temporary hack
    std::this_thread::sleep_for(std::chrono::seconds(1));
}
