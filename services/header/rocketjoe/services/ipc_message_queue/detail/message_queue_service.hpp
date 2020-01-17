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
    class message_queue_service : public boost::asio::execution_context::service {
    public:

        using key_type = message_queue_service<Implementation>;

        struct logger_impl {
            explicit logger_impl(const std::string &ident) : identifier(ident) {}

            std::string identifier;
        };


        typedef logger_impl *impl_type;

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
            impl = new logger_impl(identifier);
        }


        void destroy(impl_type &impl) {
            delete impl;
            impl = null();
        }


        void use_file(impl_type & /*impl*/, const std::string &file) {
            // Pass the work of opening the file to the background thread.
            boost::asio::post(work_io_context_, boost::bind(&message_queue_service::use_file_impl, this, file));
        }

        void log(impl_type &impl, const std::string &message) {
            // Format the text to be logged.
            std::ostringstream os;
            os << impl->identifier << ": " << message;

            // Pass the work of writing to the file to the background thread.
            boost::asio::post(work_io_context_, boost::bind(&message_queue_service::log_impl, this, os.str()));
        }

    private:

        void use_file_impl(const std::string &file) {
            ofstream_.close();
            ofstream_.clear();
            ofstream_.open(file.c_str());
        }

        void log_impl(const std::string &text) {
            ofstream_ << text << std::endl;
        }

        boost::asio::io_context work_io_context_;

        boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_;


        boost::scoped_ptr<boost::thread> work_thread_;

        Implementation mq_;
    };

} // namespace services