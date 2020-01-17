#pragma once

#include <mqueue.h>
#include <unistd.h>
#include <boost/system/error_code.hpp>
#include <boost/asio/error.hpp>

namespace ipc {

    class posix_message_queue_impl final {
        using size_type = int;
    public:

        void destroy() {
            mq_close(d_);
            if (created_) {
                mq_unlink(name_.c_str());
            }
        }

        void open(const char *name, int flags, boost::system::error_code &ec) {
            name_ = name;
            if (flags & O_CREAT) {
                d_ = mq_open(name, O_CREAT | O_RDWR,S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH, 0);
                created_ = true;
            } else {
                d_ = mq_open(name, flags);
            }
            ec = check_error(d_);
        }

        void send(const void *buffer, size_type buffer_size, unsigned int priority, boost::system::error_code &ec) {
            int ret = mq_send(d_, (const char *) buffer, buffer_size, priority);
            ec = check_error(ret);
        }

        size_t receive(void *buffer, size_type buffer_size, boost::system::error_code &ec) {
            size_t bytes = mq_receive(d_, (char *) buffer, buffer_size, 0);
            ec = check_error(bytes);
            return bytes;
        }

        size_t max_msg_size() {
            mq_attr attr;
            mq_getattr(d_, &attr);
            return attr.mq_msgsize;
        }

    private:
        boost::system::error_code check_error(int d) {
            if (d < 0) {
                return (boost::asio::error::basic_errors) errno;
            }
            return boost::system::error_code();
        }

        mqd_t d_ = 0;
        bool created_ = false;
        std::string name_;
    };

}//asio