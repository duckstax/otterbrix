#pragma once

#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include <string>

namespace services {

    template<typename Service>
    class basic_message_queue : private boost::noncopyable {
    public:

        typedef Service service_type;


        typedef typename service_type::impl_type impl_type;


        explicit basic_message_queue(boost::asio::execution_context &context,const std::string &identifier)
                : service_(boost::asio::use_service<Service>(context))
                , impl_(service_.null()) {
            service_.create(impl_, identifier);
        }


        ~basic_message_queue() {
            service_.destroy(impl_);
        }

        boost::asio::io_context &get_io_context() {
            return service_.get_io_context();
        }

        void send(const void *buffer, int buffer_size, unsigned int priority) {
            boost::system::error_code ec;
            service_.send(buffer, buffer_size, priority, ec);
            boost::asio::detail::throw_error(ec, "send");
        }

        size_t receive(void *buffer, int buffer_size) {
            boost::system::error_code ec;
            size_t bytes = service_.receive(buffer, buffer_size, ec);
            boost::asio::detail::throw_error(ec, "receive");
            return bytes;
        }

        template<typename Handler>
        void async_send(const void *buffer, int buffer_size, unsigned int priority, Handler handler) {
            service_.async_send(impl_, buffer, buffer_size, priority, handler);
        }

        template<typename Handler>
        void async_receive(void *buffer, int buffer_size, Handler handler) {
            service_.async_receive(impl_, buffer, buffer_size, handler);
        }

        size_t max_msg_size() {
            std::size_t size;
            service_.max_msg_size(impl_, size);
            return size;
        }

    private:

        service_type &service_;


        impl_type impl_;
    };

}