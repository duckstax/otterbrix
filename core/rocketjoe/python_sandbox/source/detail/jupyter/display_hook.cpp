#include <rocketjoe/python_sandbox/detail/jupyter/display_hook.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <rocketjoe/python_sandbox/detail/jupyter/session.hpp>
#include <rocketjoe/python_sandbox/detail/jupyter/zmq_socket_shared.hpp>

namespace rocketjoe { namespace services { namespace detail { namespace jupyter {

using namespace pybind11::literals;

auto display_hook::set_parent(py::object self, py::dict parent) -> void {
  self.attr("parent") = std::move(parent);
}

auto display_hook::start_displayhook(py::object self) -> void {
  self.attr("msg") = py::dict("parent"_a = self.attr("parent"),
                              "content"_a = py::dict());
}

auto display_hook::write_output_prompt(py::object self) -> void {
  self.attr("msg")["content"]["execution_count"] =
      self.attr("prompt_count");
}

auto display_hook::write_format_data(py::object self, py::dict data,
                                     py::dict metadata) -> void {
  self.attr("msg")["content"]["data"] = std::move(data);
  self.attr("msg")["content"]["metadata"] = std::move(metadata);
}

auto display_hook::finish_displayhook(py::object self) -> void {
  auto sys{py::module::import("sys")};

  sys.attr("stdout").attr("flush")();
  sys.attr("stderr").attr("flush")();

  auto msg = nl::json::parse(py::module::import("json").attr("dumps")(
      self.attr("msg")
  ).cast<std::string>());

  if(!msg["content"]["data"].is_null()) {
    auto current_session{self.attr("current_session")
                             .cast<boost::intrusive_ptr<session>>()};

    current_session->send(**self.attr("iopub_socket")
                              .cast<boost::intrusive_ptr<zmq_socket_shared>>(),
                          current_session->construct_message(
                              {self.attr("topic").cast<std::string>()},
                              {{"msg_type", "execute_result"}}, std::move(msg["parent"]),
                              {}, std::move(msg["content"])
                          )
    );
  }

  self.attr("msg") = py::none();
}

}}}}