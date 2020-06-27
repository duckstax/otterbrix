#pragma once

#include <pybind11/embed.h>
#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <zmq.hpp>

#include <boost/smart_ptr/intrusive_ref_counter.hpp>

namespace components { namespace detail { namespace jupyter {

    namespace py = pybind11;

    class zmq_socket_shared final: public boost::intrusive_ref_counter<zmq_socket_shared> {
    public:
        zmq_socket_shared(zmq::socket_t socket);

        zmq::socket_t &operator*();

    private:
        zmq::socket_t socket;
    };

}}}
