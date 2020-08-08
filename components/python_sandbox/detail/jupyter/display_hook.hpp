#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <pybind11/pybind11.h>

#include <detail/jupyter/session.hpp>

#include "socket_manager.hpp"

namespace components { namespace detail { namespace jupyter {

    namespace py = pybind11;

    class display_hook final {
    public:
        display_hook(boost::intrusive_ptr<session> current_session,
                     socket_manager iopub_socket);

        auto set_execution_count(size_t execution_count) -> void;

        auto operator()(py::object value) -> void;

    private:
        boost::intrusive_ptr<session> current_session;
        socket_manager socket_manager_;
        size_t execution_count;
    };

}}}
