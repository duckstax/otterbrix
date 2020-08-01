#include <detail/jupyter/zmq_ostream.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/core/ignore_unused.hpp>

#include <detail/jupyter/session.hpp>

//The bug related to the use of RTTI by the pybind11 library has been fixed: a
//declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

namespace components { namespace detail { namespace jupyter {

    using namespace pybind11::literals;

    auto zmq_ostream::set_parent(py::object self, py::dict parent) -> void {
        self.attr("parent") = std::move(parent);
    }

    auto zmq_ostream::writable(py::object self) -> bool {
        boost::ignore_unused(self);
        return true;
    }

    auto zmq_ostream::write(py::object self, py::str string) -> void {
        auto current_session{self.attr("current_session").cast<boost::intrusive_ptr<session>>()};

        auto iopub = self.attr("iopub_socket").cast<std::function<void(const std::string&,std::vector<std::string>)>>();

        iopub("iopub",current_session->construct_message(
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

    auto zmq_ostream::flush(py::object self) -> void {
        boost::ignore_unused(self);
    }

}}}
