#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <pybind11/pybind11.h>

#include <rocketjoe/python_sandbox/detail/jupyter/session.hpp>
#include <rocketjoe/python_sandbox/detail/jupyter/zmq_socket_shared.hpp>

namespace rocketjoe { namespace services { namespace detail { namespace jupyter {

    namespace py = pybind11;

    class display_hook final {
    public:
        display_hook(boost::intrusive_ptr<session> current_session,
                     boost::intrusive_ptr<zmq_socket_shared> iopub_socket);

        auto set_execution_count(size_t execution_count) -> void;

        auto operator()(py::object value) -> void;

    private:
        boost::intrusive_ptr<session> current_session;
        boost::intrusive_ptr<zmq_socket_shared> iopub_socket;
        size_t execution_count;
    };

}}}}
