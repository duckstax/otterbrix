#include <rocketjoe/python_sandbox/detail/jupyter/zmq_ostream.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <rocketjoe/python_sandbox/detail/jupyter/session.hpp>
#include <rocketjoe/python_sandbox/detail/jupyter/zmq_socket_shared.hpp>

//The bug related to the use of RTTI by the pybind11 library has been fixed: a
//declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

namespace rocketjoe { namespace services { namespace detail { namespace jupyter {

    using namespace pybind11::literals;

    auto zmq_ostream::set_parent(py::object self, py::dict parent) -> void {
        self.attr("parent") = std::move(parent);
    }

    auto zmq_ostream::writable(py::object self) -> bool { return true; }

    auto zmq_ostream::write(py::object self, py::str string) -> void {
        auto current_session{
          self.attr("current_session").cast<boost::intrusive_ptr<session>>()};

        current_session->send(
          **self.attr("iopub_socket")
                .cast<boost::intrusive_ptr<zmq_socket_shared>>(),
          current_session->construct_message(
              {self.attr("topic").cast<std::string>()}, {{"msg_type", "stream"}},
              nl::json::parse(py::module::import("json")
                                  .attr("dumps")(self.attr("parent"))
                                  .cast<std::string>()),
              {},
              nl::json::parse(
                  py::module::import("json")
                      .attr("dumps")(
                          py::dict("name"_a = self.attr("name").cast<std::string>(),
                                   "text"_a = std::move(string)))
                      .cast<std::string>()), {}));
    }

    auto zmq_ostream::flush(py::object self) -> void {}

}}}}
