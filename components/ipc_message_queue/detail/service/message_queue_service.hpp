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

        using  impl_type = std::shared_ptr<Implementation>;

        message_queue_service(boost::asio::execution_context &context)
                : boost::asio::execution_context::service(context),
                  work_io_context_(),
                  work_(boost::asio::make_work_guard(work_io_context_)),
                  work_thread_(new boost::thread(boost::bind(&boost::asio::io_context::run, &work_io_context_))),
                  stream_descriptor_(work_io_context_){
        }


        ~message_queue_service() {
            work_.reset();
            if (work_thread_)
                work_thread_->join();
        }

        void shutdown() {}


        impl_type null() const {
            return nullptr;
        }


        void create(impl_type &impl, const std::string &identifier) {
            impl.reset(new Implementation());
            boost::system::error_code ec;
            auto id = impl->open(identifier, O_CREAT | O_RDWR,ec);
            if(!ec) {
                stream_descriptor_.assign(id);
            }
        }


        void destroy(impl_type &impl) {
            impl->destroy();
            impl.reset();
            //delete impl;
            ///impl = null();
        }


        void max_msg_size(impl_type &impl,std::size_t& size ){
            size = impl->max_msg_size();
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
            auto opaque = [=](boost::system::error_code,std::size_t) {
                if (impl) {
                    boost::system::error_code ec;
                    size_t bytes = impl->receive(buffer, buffer_size, ec);
                    handler(ec,bytes);
                } else {
                    handler(boost::asio::error::operation_aborted,0);
                }
            };

            stream_descriptor_.async_read_some(
                    boost::asio::null_buffers(),
                    opaque
            );

        }

    private:
/*
        void handleRead(boost::system::error_code ec){
            u_int pri;
            message msg;
            ssize_t receiveRet = mq_receive(mqid, (char *) &msg, sizeof(msg), &pri);
            cout << "receiveRet = " << receiveRet << endl;

            stream_descriptor_.async_read_some(
                    boost::asio::null_buffers(),
                    boost::bind(&message_queue_service::handleRead,
                                this,
                                boost::asio::placeholders::error));
        }
*/
        boost::asio::io_context work_io_context_;

        boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_;

        boost::scoped_ptr<boost::thread> work_thread_;

        boost::asio::posix::stream_descriptor stream_descriptor_;

    };

} // namespace services