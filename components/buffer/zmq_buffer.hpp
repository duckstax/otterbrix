#pragma once

#include <vector>
#include <string>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

namespace  components {

    class zmq_buffer_tt final : public boost::intrusive_ref_counter<zmq_buffer_tt> {
    public:
        zmq_buffer_tt()
            : id_("") {}
        zmq_buffer_tt(const zmq_buffer_tt&) = delete;
        zmq_buffer_tt& operator=(const zmq_buffer_tt&) = delete;

        zmq_buffer_tt(const std::string& fd, std::vector<std::string> msgs)
            : id_(fd)
            , msg_(std::move(msgs)) {}

        const std::string& id() const {
            return id_;
        }

        const std::vector<std::string>& msg() const {
            return msg_;
        }

    private:
        const std::string id_;
        std::vector<std::string> msg_;
    };

    using zmq_buffer_t = boost::intrusive_ptr<zmq_buffer_tt>;

    template<class... Args>
    auto buffer(Args&&... args) -> zmq_buffer_t {
        return boost::intrusive_ptr<zmq_buffer_tt>(new zmq_buffer_tt(std::forward<Args>(args)...));
    }
}