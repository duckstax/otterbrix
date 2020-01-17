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


        void use_file(const std::string &file) {
            service_.use_file(impl_, file);
        }


        void log(const std::string &message) {
            service_.log(impl_, message);
        }

    private:

        service_type &service_;


        impl_type impl_;
    };

}