#include <boost/intrusive_ptr.hpp>
#include <boost/optional.hpp>
#include <jupyter/socket_manager.hpp>
#include <iostream>
#include <pybind11/pybind11.h>
#include <zmq_addon.hpp>

//The bug related to the use of RTTI by the pybind11 library has been fixed: a
//declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

namespace services { namespace interactive_python_interpreter { namespace jupyter {

    socket_manager_t::socket_manager_t(std::function<void(const std::string&, std::vector<std::string>)> f, zmq::socket_ref stdin_socket)
        : zmq_(std::move(f))
        , stdin_(stdin_socket) {}

    void socket_manager_t::socket(const std::string& socket_type, std::vector<std::string> msgs) {
        zmq_(socket_type, std::move(msgs));
    }

    void socket_manager_t::iopub(std::vector<std::string> msgs) {
        zmq_("iopub", msgs);
    }

    std::string socket_manager_t::stdin_socket(std::function<std::string(const std::vector<std::string>&)> f) {
        std::vector<zmq::message_t> msgs;

        if (zmq::recv_multipart(*stdin_, std::back_inserter(msgs))) {
            std::vector<std::string> msgs_for_parse;

            msgs_for_parse.reserve(msgs.size());

            for (const auto& msg : msgs) {
                std::cerr << "Stdin: " << msg << std::endl;
                msgs_for_parse.push_back(std::move(msg.to_string()));
            }

            return f(msgs_for_parse);
        }
    }
    void socket_manager_t::stdin_socket(std::vector<std::string> msgs) {
        std::vector<zmq::const_buffer> msgs_for_send;

        msgs_for_send.reserve(msgs.size());

        for (const auto& msg : msgs) {
            std::cerr << msg << std::endl;
            msgs_for_send.push_back(zmq::buffer(std::move(msg)));
        }

        auto result = zmq::send_multipart(*stdin_, std::move(msgs_for_send));

        if (!result) {
            throw std::logic_error("Error sending ZeroMQ message");
        }
    }

    socket_manager_t::socket_manager_t(std::function<void(const std::string&, std::vector<std::string>)> f)
        : zmq_(std::move(f)){}

}}} // namespace components::detail::jupyter