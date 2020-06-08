#include "zmq_hub.hpp"
namespace services {

zmq_server_t::zmq_server_t()
    : enabled_(true){}

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

    auto index = fd_index_[buffer.id()];
    auto* socket = polls_table_[index].socket;

    assert(zmq::send_multipart(zmq::socket_ref(zmq::from_handle,socket), std::move(msgs_for_send)));
}

auto zmq_server_t::add_lisaner(zmq::pollitem_t client){
    polls_table_.emplace_back(std::move(client));
    auto index = polls_table_.size();
    auto fd = polls_table_[index].fd;
    fd_index_.emplace(fd,index);
    return fd;
}

void zmq_server_t::stop() {
    enabled_ = false;
}

zmq_hub_t::zmq_hub_t(goblin_engineer::components::root_manager *ptr, components::log_t &,std::unique_ptr<zmq::context_t> ctx )
    : goblin_engineer::abstract_manager_service(ptr,"zmq_hub"){}

void zmq_hub_t::enqueue(goblin_engineer::message, actor_zeta::executor::execution_device *) {    }

void zmq_hub_t::write(zmq_buffer_t &buffer) {
    auto it_client = clients_.find(buffer.id());
    if(it_client!=clients_.end()){
        zmq_client_.write(buffer);
        return ;
    }
    auto it_lisaner = lisaner_.find(buffer.id());
    if(it_lisaner!=lisaner_.end()){
        zmq_server_.write(buffer);
    }
}

auto zmq_hub_t::add_client(zmq::pollitem_t client) {
   auto fd = zmq_client_.add_client(std::move(client));
   clients_.emplace(fd);
}

auto zmq_hub_t::add_lisaner(zmq::pollitem_t client) {
    auto fd = zmq_server_.add_lisaner(std::move(client));
    lisaner_.emplace(fd);
}

auto zmq_client_t::add_client(zmq::pollitem_t client)-> int {
    clients_.emplace_back(std::move(client));
    auto index = clients_.size();
    auto fd = clients_[index].fd;
    fd_index_.emplace(fd,index);
    return fd;
}

auto zmq_client_t::write(zmq_buffer_t &buffer) -> void {
    std::vector<zmq::const_buffer> msgs_for_send;

    msgs_for_send.reserve(buffer.msg().size());

    for (const auto& i : buffer.msg()) {
        //std::cerr << msg << std::endl;
        msgs_for_send.push_back(zmq::buffer(std::move(i)));
    }

    auto index = fd_index_[buffer.id()];
    auto* socket = clients_[index].socket;

    assert(zmq::send_multipart(zmq::socket_ref(zmq::from_handle,socket), std::move(msgs_for_send)));
}

}
