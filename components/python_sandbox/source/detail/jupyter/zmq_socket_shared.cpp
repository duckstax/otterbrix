#include <detail/jupyter/zmq_socket_shared.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>

namespace rocketjoe { namespace services { namespace detail { namespace jupyter {

    zmq_socket_shared::zmq_socket_shared(zmq::socket_t socket)
        : socket{std::move(socket)} {}

    zmq::socket_t &zmq_socket_shared::operator*() { return socket; }

}}}}
