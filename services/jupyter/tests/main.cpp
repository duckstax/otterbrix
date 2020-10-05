#include <actor-zeta/core.hpp>

#include <components/log/log.hpp>

#include <goblin-engineer/abstract_service.hpp>
#include <goblin-engineer/components/root_manager.hpp>

#include <gtest/gtest.h>

#include <services/zmq-hub/zmq_hub.hpp>

using namespace goblin_engineer;
using namespace std::chrono_literals;

class controller_t final : public abstract_service {
public:
    template<class Manager>
    controller_t(actor_zeta::intrusive_ptr<Manager> ptr)
        : abstract_service(ptr, "controller") {
        add_handler("dispatcher", &controller_t::dispatcher);
    }

    void dispatcher(services::zmq_buffer_t& buf) {
        auto& msg = static_cast<actor_zeta::context&>(*this).current_message();
        ASSERT_EQ(std::string("Hello"), buf->msg()[0]);
        std::vector<std::string> msgs = {"World"};
        actor_zeta::send(msg.sender(),actor_zeta::actor_address(this),"write",services::buffer(buf->id(),msgs));
    }
};

void work(zmq::context_t& ctx) {
    zmq::socket_t socket(ctx, ZMQ_REQ);
    socket.connect("tcp://localhost:9999");
    try {
        auto msgs_for_send = {zmq::buffer(std::string("Hello"))};
        ASSERT_TRUE(zmq::send_multipart(zmq::socket_ref(zmq::from_handle, socket), std::move(msgs_for_send)));
        std::vector<zmq::message_t> omsgs;
        ASSERT_TRUE(zmq::recv_multipart(socket, std::back_inserter(omsgs)));
        socket.close();
    } catch (std::exception const& e) {
        std::cerr << " Work Run exception : " << e.what() << std::endl;
    }
}

TEST(zmq_hub_server, ping_pong) {
    auto ctx = std::make_unique<zmq::context_t>();
    auto log = ::components::initialization_logger();
    goblin_engineer::components::root_manager env(1, 1000);
    auto zmq_hub = goblin_engineer::components::make_manager_service<services::zmq_hub_t>(env, log);
    auto controller = goblin_engineer::components::make_service<controller_t>(zmq_hub);
    services::make_listener_zmq_socket(*ctx, zmq_hub, services::make_url("tcp", "*", 9999), zmq::socket_type::rep, controller);
    zmq_hub->run();
    auto& ctx_ref = *ctx;
    std::thread t1([&ctx_ref]() {
        work(ctx_ref);
    });
    t1.join();
    zmq_hub->stop();
    ctx->close();
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}