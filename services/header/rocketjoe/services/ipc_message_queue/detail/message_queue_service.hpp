#pragma once

#include <boost/asio.hpp>
#include <boost/thread/thread.hpp>
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/noncopyable.hpp>
#include <boost/scoped_ptr.hpp>
#include <fstream>
#include <sstream>
#include <string>

namespace services {

    template <class Implementation>
    class message_queue_service final : public boost::asio::execution_context::service {
    public:

        using key_type = message_queue_service<Implementation>;

        using  impl_type = Implementation*;

        message_queue_service(boost::asio::execution_context &context)
                : boost::asio::execution_context::service(context),
                  work_io_context_(),
                  work_(boost::asio::make_work_guard(work_io_context_)),
                  work_thread_(new boost::thread(boost::bind(&boost::asio::io_context::run, &work_io_context_))) {
        }


        ~message_queue_service() {
            work_.reset();
            if (work_thread_)
                work_thread_->join();
        }

        void shutdown() {}


        impl_type null() const {
            return 0;
        }


        void create(impl_type &impl, const std::string &identifier) {
            impl = new impl_type();
            impl->open(identifier, O_CREAT | O_RDWR);
        }


        void destroy(impl_type &impl) {
            impl->close();
            delete impl;
            impl = null();
        }


        template<typename Handler>
        void async_send(impl_type &impl, const void *buffer, int buffer_size, unsigned int priority, Handler handler) {
            auto opaque = [=] {
                if (impl) {
                    boost::system::error_code ec;
                    impl->send(buffer, buffer_size, priority, ec);
                    handler(ec);
                } else {
                    handler(boost::asio::error::operation_aborted);
                }
            };

            boost::asio::post(work_io_context_,opaque);
        }

        template<typename Handler>
        void async_receive(impl_type &impl, void *buffer, int buffer_size, Handler handler) {
            auto opaque = [=]() mutable {
                if (impl) {
                    boost::system::error_code ec;
                    size_t bytes = impl->receive(buffer, buffer_size, ec);
                    boost::asio::post(work_.get_executor(),boost::asio::detail::bind_handler(handler, ec, bytes));
                } else {
                    boost::asio::post(work_.get_executor(),boost::asio::detail::bind_handler(handler, boost::asio::error::operation_aborted, -1));
                }
            };

            boost::asio::post(work_io_context_,opaque);
        }

    private:

        boost::asio::io_context work_io_context_;

        boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_;

        boost::scoped_ptr<boost::thread> work_thread_;

    };

} // namespace services