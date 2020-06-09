#include "zmq_hub.hpp"
#include <actor-zeta/core.hpp>
namespace services {

    zmq_server_t::zmq_server_t()
        : enabled_(true) {}

    zmq_server_t::~zmq_server_t() {
        stop();
        thread_.join();
        std::cerr << "~zmq_server_t" << std::endl;
    }

    void zmq_server_t::run_() {
        std::cerr << " run_ " << std::endl;
        while (enabled_) {
            std::cerr << " while " << std::endl;
            std::unique_lock<std::mutex>_(mtx_);
            {
                std::cerr << " mtx " << std::endl;
                std::swap(task_,inner_task_);
            }
            std::cerr << "  while(!inner_task_.empty()){ " << std::endl;
            while(!inner_task_.empty()){
                auto tmp = std::move(inner_task_.back());
                inner_task_.pop();
                inner_write(std::move(tmp));
            }

            std::cerr << " if (zmq::poll(polls_table_) == -1) { " << std::endl;
            auto d = zmq::poll(polls_table_);
            std::cerr << "status :" << d << std::endl;
            if (d == -1) {
                std::cerr << "  continue " << std::endl;
                continue;
            }

            std::cerr << "   for (auto& i : polls_table_) { " << std::endl;
            for (auto& i : polls_table_) {
                if (i.revents & ZMQ_POLLIN) {
                    std::vector<zmq::message_t> msgs;
                    auto result = zmq::recv_multipart(
                        zmq::socket_ref(zmq::from_handle, i.socket),
                        std::back_inserter(msgs),
                        zmq::recv_flags::dontwait);
                    std::cerr << "result : " << *result << std::endl;
                    while (result) {
                        std::vector<std::string> msgs_for_parse;

                        msgs_for_parse.reserve(msgs.size());

                        for (const auto& msg : msgs) {
                            msgs_for_parse.push_back(msg.to_string());
                        }

                        auto index = fd_index_[i.fd];
                        senders_[index](zmq_buffer_t(i.fd, std::move(msgs_for_parse)));

                    } /// while
                }
            } /// for

        } /// while
    }

    void zmq_server_t::write(zmq_buffer_t& buffer) {
        std::unique_lock<std::mutex>_(mtx_);
        {
            task_.emplace(std::move(buffer));
        }
    }

    auto zmq_server_t::add_listener(zmq::pollitem_t client, sender_t current_sender) {
        polls_table_.emplace_back(std::move(client));
        senders_.emplace_back(std::move(current_sender));
        auto index = polls_table_.size();
        auto fd = polls_table_[index].fd;
        fd_index_.emplace(fd, index);
        return fd;
    }

    void zmq_server_t::stop() {
        enabled_ = false;
    }
    void zmq_server_t::run() {
        thread_ = std::thread([this](){
            try {
                run_();
            } catch (std::exception const & e){
                std::cerr << "Run exception" <<e.what() << std::endl;
            }

        });
    }
    void zmq_server_t::inner_write(zmq_buffer_t buffer) {
        zmq_buffer_t tmp_buffer = std::move(buffer);
        std::vector<zmq::const_buffer> msgs_for_send;

        msgs_for_send.reserve(tmp_buffer.msg().size());

        for (const auto& i : tmp_buffer.msg()) {
            //std::cerr << msg << std::endl;
            msgs_for_send.push_back(zmq::buffer(std::move(i)));
        }

        auto index = fd_index_[tmp_buffer.id()];
        auto* socket = polls_table_[index].socket;

        assert(zmq::send_multipart(zmq::socket_ref(zmq::from_handle, socket), std::move(msgs_for_send)));
    }

    zmq_hub_t::zmq_hub_t(goblin_engineer::components::root_manager* ptr, components::log_t&, std::unique_ptr<zmq::context_t> ctx)
        : goblin_engineer::abstract_manager_service(ptr, "zmq_hub")
        , init_(true)
        , ctx_(std::move(ctx)) {
        add_handler("write", &zmq_hub_t::write);
        std::cerr << "constructor" << std::endl;
    }

    void zmq_hub_t::enqueue(goblin_engineer::message msg, actor_zeta::executor::execution_device*) {
        set_current_message(std::move(msg));
        dispatch().execute(*this);
    }

    void zmq_hub_t::write(zmq_buffer_t& buffer) {
        auto it_client = clients_.find(buffer.id());
        if (it_client != clients_.end()) {
            zmq_client_.write(buffer);
            return;
        }

        auto it_lisaner = listener_.find(buffer.id());
        if (it_lisaner != listener_.end()) {
            zmq_server_.write(buffer);
            return;
        }
    }

    auto zmq_hub_t::add_client(zmq::pollitem_t client) {
        if(init_) {
            auto fd = zmq_client_.add_client(std::move(client));
            clients_.emplace(fd);
        } else {
            throw std::runtime_error("non corector add lisaner");
        }
    }

    auto zmq_hub_t::add_listener(zmq::pollitem_t client,actor_zeta::detail::string_view name ) -> void {
        if(init_) {
            auto fd = zmq_server_.add_listener(
                std::move(client),
                [this, name](zmq_buffer_t buffer) {
                    actor_zeta::send(addresses(name), actor_zeta::actor_address(this), "dispatcher", std::move(buffer));
                });
            listener_.emplace(fd);
        } else {
            throw std::runtime_error("non corector add lisaner");
        }
    }

    zmq::context_t& zmq_hub_t::ctx() {
        if(init_) {
            return *ctx_;
        } else {
            throw std::runtime_error("non corector add lisaner");
        }
    }
    void zmq_hub_t::run() {
        if(init_) {
            init_= false;
            zmq_server_.run();
        } else {
            throw std::runtime_error("non corector add lisaner");
        }
    }

    auto zmq_client_t::add_client(zmq::pollitem_t client) -> int {
        clients_.emplace_back(std::move(client));
        auto index = clients_.size();
        auto fd = clients_[index].fd;
        fd_index_.emplace(fd, index);
        return fd;
    }

    auto zmq_client_t::write(zmq_buffer_t& buffer) -> void {
        std::vector<zmq::const_buffer> msgs_for_send;

        msgs_for_send.reserve(buffer.msg().size());

        for (const auto& i : buffer.msg()) {
            //std::cerr << msg << std::endl;
            msgs_for_send.push_back(zmq::buffer(std::move(i)));
        }

        auto index = fd_index_[buffer.id()];
        auto* socket = clients_[index].socket;

        assert(zmq::send_multipart(zmq::socket_ref(zmq::from_handle, socket), std::move(msgs_for_send)));
    }

} // namespace services
