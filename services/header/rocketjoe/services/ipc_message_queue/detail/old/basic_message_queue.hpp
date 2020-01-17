#pragma once

#include <boost/asio.hpp>
#include <cstddef>

namespace ipc {

    template<typename Service>
    class basic_message_queue  final : public boost::asio::basic_io_object<Service> {
    public:
        basic_message_queue(boost::asio::io_service &io_service, const char *name)
                : basic_message_queue(io_service, name, O_CREAT | O_RDWR) {
        }

        basic_message_queue(boost::asio::io_service &io_service, const char *name, int flags)
                : boost::asio::basic_io_object<Service>(io_service),
                  name_(name) {
            boost::system::error_code ec;
            this->get_implementation()->open(name, flags, ec);
            boost::asio::detail::throw_error(ec, "open");
        }

        void send(const void *buffer, int buffer_size, unsigned int priority) {
            boost::system::error_code ec;
            this->get_implementation()->send(buffer, buffer_size, priority, ec);
            boost::asio::detail::throw_error(ec, "send");
        }

        size_t receive(void *buffer, int buffer_size) {
            boost::system::error_code ec;
            size_t bytes = this->get_implementation()->receive(buffer, buffer_size, ec);
            boost::asio::detail::throw_error(ec, "receive");
            return bytes;
        }

        template<typename Handler>
        void async_send(const void *buffer, int buffer_size, unsigned int priority, Handler handler) {
            this->get_service().async_send(this->get_implementation(), buffer, buffer_size, priority, handler);
        }

        template<typename Handler>
        void async_receive(void *buffer, int buffer_size, Handler handler) {
            this->get_service().async_receive(this->get_implementation(), buffer, buffer_size, handler);
        }

        size_t max_msg_size() {
            return this->get_implementation()->max_msg_size();
        }

    private:
        std::string name_;
    };

}