#pragma once

#include <thread>


#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>

namespace ipc {

    template<typename MqueueImplementation>
    class basic_message_queue_service final : public boost::asio::execution_context::service {
    public:
        static boost::asio::io_service::id id;

        explicit basic_message_queue_service(boost::asio::execution_context& context)
            : boost::asio::execution_context::service(context)
            , context_()
            , work_(boost::asio::make_work_guard(context_))
            , work_thread_(new std::thread(
                    [this](){
                        context_.run();
                    }
            )){}

        ~basic_message_queue_service() override {
            work_.reset();
            if (work_thread_)
                work_thread_->join();
        }

        using implementation_type = boost::shared_ptr<MqueueImplementation>;

        void construct(implementation_type &impl) {
            impl.reset(new MqueueImplementation());
        }

        void move_construct(implementation_type &impl,
                            implementation_type &other_impl) {
            impl = std::move(other_impl);
        }


        void destroy(implementation_type &impl) {
            if (impl) {
                impl->destroy();
            }
            impl.reset();
        }

        template<typename Handler>
        void async_send(implementation_type &impl, const void *buffer, int buffer_size, unsigned int priority, Handler handler) {
            std::weak_ptr<MqueueImplementation> weak_impl(impl);

            auto opaque = [=] {
                implementation_type impl = weak_impl.lock();
                if (impl) {
                    boost::system::error_code ec;
                    impl->send(buffer, buffer_size, priority, ec);
                    handler(ec);
                } else {
                    handler(boost::asio::error::operation_aborted);
                }
            };

            boost::asio::post(context_,opaque);
        }

        template<typename Handler>
        void async_receive(implementation_type &impl, void *buffer, int buffer_size, Handler handler) {
            std::weak_ptr<MqueueImplementation> weak_impl(impl);
            boost::asio::io_service::work work(this->get_context());

            auto opaque = [=]() mutable {
                implementation_type impl = weak_impl.lock();
                if (impl) {
                    boost::system::error_code ec;
                    size_t bytes = impl->receive(buffer, buffer_size, ec);
                    work.get_io_context().post(boost::asio::detail::bind_handler(handler, ec, bytes));
                } else {
                    work.get_io_context().post(boost::asio::detail::bind_handler(handler, boost::asio::error::operation_aborted, -1));
                }
            };

            context_.post(opaque);
        }

    private:

        boost::asio::io_context context_;
        boost::asio::executor_work_guard<boost::asio::io_context::executor_type>  work_;
        std::unique_ptr<std::thread> work_thread_;
    };

    template<typename MqueueImplementation>
    boost::asio::io_service::id basic_message_queue_service<MqueueImplementation>::id;

}
