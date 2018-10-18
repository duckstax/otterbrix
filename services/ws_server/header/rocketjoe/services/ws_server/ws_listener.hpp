#pragma once

#include <algorithm>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <chrono>

#include <boost/beast/core.hpp>
#include <boost/beast/version.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <rocketjoe/services/ws_server/ws_session.hpp>
#include <goblin-engineer/abstract_service.hpp>
#include <rocketjoe/api/websocket.hpp>

namespace rocketjoe { namespace services { namespace ws_server {

            using tcp = boost::asio::ip::tcp;
            namespace http = boost::beast::http;
            namespace websocket = boost::beast::websocket;

            using clock = std::chrono::steady_clock;

            class ws_listener : public std::enable_shared_from_this<ws_listener> {
            public:
                ws_listener(
                        boost::asio::io_context &ioc,
                        tcp::endpoint endpoint,
                        goblin_engineer::pipe* pipe_
                );

                void write(std::unique_ptr<api::web_socket>);

                void run();

                void do_accept();

                void on_accept(boost::system::error_code ec);

            private:
                boost::asio::strand<boost::asio::io_context::executor_type> strand_;
                tcp::acceptor acceptor_;
                tcp::socket socket_;
                goblin_engineer::pipe* pipe_;
                std::unordered_map<api::transport_id,std::shared_ptr<ws_session>> storage_sessions;
            };


        }
    }
}