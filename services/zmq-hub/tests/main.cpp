#include "gtest/gtest.h"
#include <services/zmq-hub/zmq_hub.hpp>
#include <goblin-engineer/abstract_service.hpp>
#include <goblin-engineer/components/root_manager.hpp>
#include <components/log/log.hpp>

using namespace goblin_engineer;


class controller_t final : public abstract_service {
public:
    template<class Manager>
    controller_t(actor_zeta::intrusive_ptr<Manager> ptr)
        : abstract_service(ptr, "controller") {
        add_handler("dispatcher", &controller_t::dispatcher);
        std::cerr << "constructor 1" << std::endl;
    }

    void dispatcher(services::zmq_buffer_t& buffer) {
        //auto msg = std::move( static_cast<actor_zeta::context&>(*this).current_message());
        for (auto& i : buffer.msg()) {
            ASSERT_EQ("Hello",buffer.msg()[0]);
        }
    }
};

void work(zmq::context_t&ctx) {
    std::cerr << "start" <<std::endl;

    zmq::socket_t socket (ctx, ZMQ_REQ);
    socket.connect ("tcp://localhost:9999");
    std::cerr << "connect" <<std::endl;
    zmq::message_t request (5);
    memcpy (request.data (), "Hello", 5);
    try {
        socket.send (request);
    } catch (std::exception const & e){
        std::cerr << "Run exception" <<e.what() << std::endl;
    }

}

using namespace std::chrono_literals;
/*
TEST(zmq_hub_server, ping_pong) {

    auto log = ::components::initialization_logger();
    goblin_engineer::components::root_manager env(1, 1000);
    auto zmq_hub = goblin_engineer::components::make_manager_service<services::zmq_hub_t>(env, log, std::move(std::make_unique<zmq::context_t>()));
    auto controller = goblin_engineer::components::make_service<controller_t>(zmq_hub);
    services::make_listener_zmq_socket(zmq_hub, services::make_url("tcp", "localhost", 9999), zmq::socket_type::router, controller);

    std::this_thread::sleep_for(10s);

    std::thread t1([](){
        work();
    });

    std::this_thread::sleep_for(30s);

    t1.join();
}
 */
/*
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
 */

int main() {
    auto ctx = std::make_unique<zmq::context_t>();
    auto log = ::components::initialization_logger();
    goblin_engineer::components::root_manager env(1, 1000);
    auto zmq_hub = goblin_engineer::components::make_manager_service<services::zmq_hub_t>(env, log);
    auto controller = goblin_engineer::components::make_service<controller_t>(zmq_hub);
    services::make_listener_zmq_socket(*ctx,zmq_hub, services::make_url("tcp", "*", 9999), zmq::socket_type::router, controller);
    zmq_hub->run();
    std::this_thread::sleep_for(5s);
    auto& ctx_ref = *ctx;
    std::thread t1([&ctx_ref](){
      work(ctx_ref);
    });

    std::this_thread::sleep_for(30s);

    t1.join();
    return 0;
}