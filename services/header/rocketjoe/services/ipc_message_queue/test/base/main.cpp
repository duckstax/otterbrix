#include <iostream>
#include "../../detail/message_queue.hpp"

static std::string in_data;

void received_handler(const boost::system::error_code &ec, size_t bytes) {
    in_data.resize(bytes);
    std::cerr << __PRETTY_FUNCTION__ << " " << ec << std::endl;
}

void sent_handler(const boost::system::error_code &ec) {
    std::cerr << __PRETTY_FUNCTION__ << " " << ec << std::endl;
}

void test1() {
    boost::asio::io_service io;

    std::string out_data("hello I ma mq2");

    services::message_queue mq(io, "/server");

    in_data.resize(mq.max_msg_size());

    mq.async_receive((void *) in_data.data(), in_data.capacity(), received_handler);

    services::message_queue mq2(io, "/server");
    mq2.async_send(out_data.data(), out_data.size(), 0, sent_handler);

    auto d = io.run();
    std::cerr<< d;

    std::cerr << in_data << " " << in_data.size() << std::endl;
    std::cerr << out_data << " " << out_data.size() << std::endl;
    ///assert(in_data == out_data);
}

void test2() {
    boost::asio::io_service io;

    std::string out_data("hello I myself");

    services::message_queue mq(io, "/server");

    in_data.resize(mq.max_msg_size());
    mq.async_receive((void *) in_data.data(), in_data.capacity(), received_handler);;
    mq.async_send(out_data.data(), out_data.size(), 0, sent_handler);

    io.run();

    std::cout << in_data << " " << in_data.size() << std::endl;
    std::cout << out_data << " " << out_data.size() << std::endl;
    assert(in_data == out_data);
}


int main() {
    test1();
    test2();
    return 0;
}