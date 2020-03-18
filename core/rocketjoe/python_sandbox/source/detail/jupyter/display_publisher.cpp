#include <rocketjoe/python_sandbox/detail/jupyter/display_publisher.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <rocketjoe/python_sandbox/detail/jupyter/session.hpp>
#include <rocketjoe/python_sandbox/detail/jupyter/zmq_socket_shared.hpp>

//The bug related to the use of RTTI by the pybind11 library has been fixed: a
//declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

namespace rocketjoe { namespace services { namespace detail { namespace jupyter {

    using namespace pybind11::literals;

    auto display_publisher::set_parent(py::object self,
                                       py::dict parent) -> void {
        self.attr("parent") = std::move(parent);
    }

    auto display_publisher::publish(py::object self, py::dict data,
                                    py::dict metadata, py::str source,
                                    py::dict trasistent,
                                    py::bool_ update) -> void {
        auto sys{py::module::import("sys")};

        sys.attr("stdout").attr("flush")();
        sys.attr("stderr").attr("flush")();

        if(!metadata) {
            metadata = py::dict();
        }

        self.attr("_validate_data")(data, metadata);

        if(!trasistent) {
            trasistent = py::dict();
        }

        auto current_session{self.attr("current_session")
                               .cast<boost::intrusive_ptr<session>>()};

        current_session->send(**self.attr("iopub_socket")
                                .cast<boost::intrusive_ptr<zmq_socket_shared>>(),
                            current_session->construct_message(
                                {self.attr("topic").cast<std::string>()},
                                {{"msg_type", update ? "update_display_data" : "display_data"}},
                                nl::json::parse(py::module::import("json").attr("dumps")(
                                    self.attr("parent")
                                ).cast<std::string>()), {},
                                nl::json::parse(py::module::import("json").attr("dumps")(
                                    py::dict("data"_a = std::move(data),
                                             "metadata"_a = std::move(metadata),
                                             "transistent"_a = std::move(trasistent))
                                ).cast<std::string>()), {}
                            )
        );
    }

    auto display_publisher::clear_output(py::object self,
                                         py::bool_ wait) -> void {
        auto sys{py::module::import("sys")};

        sys.attr("stdout").attr("flush")();
        sys.attr("stderr").attr("flush")();

        auto current_session{self.attr("current_session")
                               .cast<boost::intrusive_ptr<session>>()};

        current_session->send(**self.attr("iopub_socket")
                                .cast<boost::intrusive_ptr<zmq_socket_shared>>(),
                            current_session->construct_message(
                                {self.attr("topic").cast<std::string>()},
                                {{"msg_type", "clear_output"}},
                                nl::json::parse(py::module::import("json").attr("dumps")(
                                    self.attr("parent")
                                ).cast<std::string>()), {},
                                nl::json::parse(py::module::import("json").attr("dumps")(
                                    py::dict("wait"_a = wait)
                                ).cast<std::string>()), {}
                            )
        );
    }

}}}}
