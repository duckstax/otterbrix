#pragma once

#include <algorithm>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <string>

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <goblin-engineer/abstract_service.hpp>
#include <rocketjoe/api/transport_base.hpp>

namespace rocketjoe { namespace data_provider { namespace ws_server {

    using tcp = boost::asio::ip::tcp;               // from <boost/asio/ip/tcp.hpp>
    namespace http = boost::beast::http;            // from <boost/beast/http.hpp>
    namespace websocket = boost::beast::websocket;  // from <boost/beast/websocket.hpp>

    inline void fail(boost::system::error_code ec, char const* what) {
        std::cerr << (std::string(what) + ": " + ec.message() + "\n");
    }


    template<class NextLayer>
    inline void setup_stream(websocket::stream<NextLayer>& ws) {
        websocket::permessage_deflate pmd;
        pmd.client_enable = true;
        pmd.server_enable = true;
        pmd.compLevel = 3;
        ws.set_option(pmd);
        ws.auto_fragment(false);
        ws.read_message_max(64 * 1024 * 1024);
    }

    class ws_session : public std::enable_shared_from_this<ws_session> {
    public:
        explicit ws_session(
                tcp::socket,
                api::transport_id ,
                actor_zeta::actor::actor_address
        );

        void run();

        void write(std::unique_ptr<api::transport_base>);

        void on_accept(boost::system::error_code ec);

        void do_read();

        void on_read(boost::system::error_code ec, std::size_t bytes_transferred);

        void on_write(boost::system::error_code ec, std::size_t bytes_transferred);

        const size_t id_;
    private:

        websocket::stream<tcp::socket> ws_;
        boost::asio::strand<boost::asio::io_context::executor_type> strand_;
        boost::beast::multi_buffer buffer_;
        actor_zeta::actor::actor_address pipe_;
    };

}}}