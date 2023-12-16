#include <catch2/catch.hpp>

#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include <actor-zeta.hpp>
#include <actor-zeta/detail/memory_resource.hpp>

auto thread_pool_deleter = [](actor_zeta::scheduler_abstract_t* ptr) {
    ptr->stop();
    delete ptr;
};

using actor_zeta::detail::pmr::memory_resource;

class supervisor_lite final : public actor_zeta::cooperative_supervisor<supervisor_lite> {
public:
    enum class system_command : std::uint64_t { any_method };

    explicit supervisor_lite(memory_resource* ptr)
        : cooperative_supervisor(ptr, "network")
        , e_(new actor_zeta::scheduler_t<actor_zeta::work_sharing>(1, 1000), thread_pool_deleter) {
        e_->start();
        method_ = new method_t();
        add_handler(system_command::any_method, method_, &method_t::invoke);
    }

    ~supervisor_lite() override = default;

protected:
    auto scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* final { return e_.get(); }

    auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void final {
        set_current_message(std::move(msg));
        execute(this, current_message());
    }

private:
    std::unique_ptr<actor_zeta::scheduler_abstract_t, decltype(thread_pool_deleter)> e_;
    std::vector<actor_zeta::actor> actors_;
    class method_t final {
    public:
        void invoke() { REQUIRE(true); }
    };
    method_t* method_;
};

TEST_CASE("rpc call style") {
    auto* mr_ptr = actor_zeta::detail::pmr::get_default_resource();
    auto supervisor = actor_zeta::spawn_supervisor<supervisor_lite>(mr_ptr);
    actor_zeta::send(supervisor.get(),
                     actor_zeta::address_t::empty_address(),
                     supervisor_lite::system_command::any_method);
    std::this_thread::sleep_for(std::chrono::seconds(180));
}
