#include <iostream>
#include <boost/asio/steady_timer.hpp>
#include <chrono>

#include "../../timer_manager.hpp"

using namespace std::chrono_literals;

int main(){
    boost::asio::io_context ctx;

    rocketjoe::network::timer tmp(ctx,1ms,[](){
        std::cerr << "!" << std::endl;
    });

    ctx.run();

    return 0;
}