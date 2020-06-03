#include "zmq_hub.hpp"
namespace services {

zmq_server_t::zmq_server_t(std::vector<zmq::pollitem_t> polls, int io_threads, int max_sockets)
    : enabled_(true)
    , zmq_context_(std::make_unique<zmq::context_t>(io_threads,max_sockets))
    , polls_table_(std::move(polls)){}

zmq_server_t::~zmq_server_t() { stop(); }

void zmq_server_t::run() {
    while (enabled_) {
        if (zmq::poll(polls_table_) == -1) {
            continue;
        }

        for (auto& i : polls_table_) {
            if (i.revents & ZMQ_POLLIN) {
                std::vector<zmq::message_t> msgs;
                auto result = zmq::recv_multipart(
                            zmq::socket_ref(zmq::from_handle, i.socket),
                            std::back_inserter(msgs),
                            zmq::recv_flags::dontwait);
                while (result) {
                    std::vector<std::string> msgs_for_parse;

                    msgs_for_parse.reserve(msgs.size());

                    for (const auto& msg : msgs) {
                        msgs_for_parse.push_back(msg.to_string());
                    }

                    ///if (!dispatch_shell(std::move(msgs_for_parse))) {
                    ///    return false;
                    ///}
                }
            }
        }
    }
}

void zmq_server_t::write(zmq_buffer_t &buffer) {
    std::vector<zmq::const_buffer> msgs_for_send;

    msgs_for_send.reserve(buffer.msg().size());

    for (const auto& i : buffer.msg()) {
        //std::cerr << msg << std::endl;
        msgs_for_send.push_back(zmq::buffer(std::move(i)));
    }

    auto index = fd_position_[buffer.id()];
    auto* socket = polls_table_[index].socket;

    assert(zmq::send_multipart(zmq::socket_ref(zmq::from_handle,socket), std::move(msgs_for_send)));
}

zmq_hub_t::zmq_hub_t(goblin_engineer::components::root_manager *ptr, components::log_t &,std::vector<zmq::pollitem_t> pollitems )
    : goblin_engineer::abstract_manager_service(ptr,"zmq_hub")
    , zmq_server_(std::move(pollitems)){}

void zmq_hub_t::enqueue(goblin_engineer::message, actor_zeta::executor::execution_device *) {    }

void zmq_hub_t::write(zmq_buffer_t &buffer) {
    zmq_server_.write(buffer);
}
}
