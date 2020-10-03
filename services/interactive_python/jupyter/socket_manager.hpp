#pragma once

#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/optional.hpp>

#include <functional>

#include <zmq.hpp>

namespace services { namespace interactive_python_interpreter { namespace jupyter {

    class socket_manager_t final : public boost::intrusive_ref_counter<socket_manager_t> {
    public:
        socket_manager_t(std::function<void(const std::string&, std::vector<std::string>)>, zmq::socket_ref stdin_socket);

        socket_manager_t(std::function<void(const std::string&, std::vector<std::string>)> f);

        ~socket_manager_t() = default;

        void socket(const std::string& socket_type, std::vector<std::string> msgs);

        void iopub(std::vector<std::string> msgs);

        void stdin_socket(std::vector<std::string> msgs);

        std::string stdin_socket(std::function<std::string(const std::vector<std::string>&)> f);

    private:
        std::function<void(const std::string& socket_type, std::vector<std::string>)> zmq_;
        boost::optional<zmq::socket_ref> stdin_;
    };

    using socket_manager = boost::intrusive_ptr<socket_manager_t>;

    template<typename... Args>
    auto make_socket_manager(Args&&... args) -> socket_manager {
        return new socket_manager_t(std::forward<Args>(args)...);
    }

}}} // namespace components::detail::jupyter