#include <rocketjoe/python_sandbox/detail/jupyter/shell.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <rocketjoe/python_sandbox/detail/jupyter/display_hook.hpp>
#include <rocketjoe/python_sandbox/detail/jupyter/display_publisher.hpp>
#include <rocketjoe/python_sandbox/detail/jupyter/session.hpp>
#include <rocketjoe/python_sandbox/detail/jupyter/zmq_ostream.hpp>
#include <rocketjoe/python_sandbox/detail/jupyter/zmq_socket_shared.hpp>

namespace rocketjoe { namespace services { namespace detail { namespace jupyter {

using namespace pybind11::literals;

auto shell::_default_banner1(py::object self) -> py::object {
  return py::module::import("IPython.core.usage").attr("default_banner");
}

auto shell::_default_exiter(py::object self) -> py::object {
  return py::module::import("IPython.core.autocall")
      .attr("ZMQExitAutocall")(std::move(self));
}

auto shell::init_hooks(py::object self) -> void {
  py::module::import("IPython.core.interactiveshell")
      .attr("InteractiveShell").attr("init_hooks")(self);
  self.attr("set_hook")(
      "show_in_pager", py::module::import("IPython.core").attr("page")
          .attr("as_hook")(py::module::import("IPython.core.payloadpage")
                               .attr("page")), 99
  );
}

auto shell::ask_exit(py::object self) -> void {
  auto keepkernel_on_exit{self.attr("keepkernel_on_exit")};

  self.attr("exit_now") = !keepkernel_on_exit.cast<py::bool_>();
  self.attr("payload_manager").attr("write_payload")(
      py::dict("source"_a = "ask_exit",
               "keepkernel"_a = keepkernel_on_exit)
  );
}

auto shell::run_cell(py::object self, py::args args,
                     py::kwargs kwargs) -> py::object {
  self.attr("_last_traceback") = py::none();

  return py::module::import("IPython.core.interactiveshell")
      .attr("InteractiveShell").attr("run_cell")(self, *args, **kwargs);
}

auto shell::_showtraceback(py::object self, py::object etype,
                           py::object evalue, py::object stb) -> void {
  auto sys{py::module::import("sys")};

  sys.attr("stdout").attr("flush")();
  sys.attr("stderr").attr("flush")();

  auto displayhook{self.attr("displayhook")};
  auto topic{py::none()};

  if(!displayhook.attr("topic").is_none()) {
    topic = displayhook.attr("topic").attr("replace")("execute_result",
                                                      "error");
  }

  auto current_session{displayhook.attr("current_session")
                           .cast<boost::intrusive_ptr<session>>()};

  current_session->send(**displayhook.attr("iopub_socket")
                            .cast<boost::intrusive_ptr<zmq_socket_shared>>(),
                        current_session->construct_message(
                            {topic.cast<std::string>()}, {{"msg_type", "error"}},
                            nl::json::parse(py::module::import("json").attr("dumps")(
                                displayhook.attr("parent")
                            ).cast<std::string>()), {},
                            nl::json::parse(py::module::import("json").attr("dumps")(
                                py::dict("traceback"_a = stb,
                                         "ename"_a = etype.attr("__name__"),
                                         "evalue"_a = py::str(std::move(evalue)))
                            ).cast<std::string>())
                        )
  );

  self.attr("_last_traceback") = std::move(stb);
}

auto shell::set_next_input(py::object self, py::object text,
                           py::bool_ replace) -> void {
  self.attr("payload_manager").attr("write_payload")(
      py::dict("source"_a = "set_next_input", "text"_a = std::move(text),
               "replace"_a = replace)
  );
}

auto shell::set_parent(py::object self, py::dict parent) -> void {
  display_hook::set_parent(self.attr("displayhook"), parent);
  display_publisher::set_parent(self.attr("display_pub"), parent);

  auto sys{py::module::import("sys")};

  zmq_ostream::set_parent(sys.attr("stdout"), parent);
  zmq_ostream::set_parent(sys.attr("stderr"), parent);
}

auto shell::init_environment(py::object self) -> void {
  auto environ{py::module::import("os").attr("environ")};

  environ["TERM"] = "xterm-color";
  environ["CLICOLOR"] = "1";
  environ["PAGER"] = "cat";
  environ["GIT_PAGER"] = "cat";
}

auto shell::init_virtualenv(py::object self) -> void {}

}}}}