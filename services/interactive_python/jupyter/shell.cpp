#include <jupyter/shell.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <jupyter/shell_display_hook.hpp>
#include <jupyter/display_publisher.hpp>
#include <jupyter/session.hpp>
#include <jupyter/zmq_ostream.hpp>
#include <jupyter/socket_manager.hpp>
#include <boost/core/ignore_unused.hpp>
#include <components/log/log.hpp>

//The bug related to the use of RTTI by the pybind11 library has been fixed: a
//declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

namespace services { namespace interactive_python_interpreter { namespace jupyter {

    using namespace pybind11::literals;

    auto shell::_default_banner1(py::object self) -> py::object {
        boost::ignore_unused(self);
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
        auto log = components::get_logger();
        auto sys{py::module::import("sys")};

        sys.attr("stdout").attr("flush")();
        sys.attr("stderr").attr("flush")();
        auto displayhook{self.attr("displayhook")};
        auto topic{py::none()};
        if(!displayhook.attr("topic").is_none()) {
            topic = displayhook.attr("topic").attr("replace")("execute_result",
                                                              "error");
        }
        auto current_session{displayhook.attr("current_session").cast<boost::intrusive_ptr<session>>()};
        auto sm = displayhook.attr("iopub_socket").cast<socket_manager>();
        sm->iopub(current_session->construct_message(
                                {topic.cast<std::string>()}, {{"msg_type", "error"}},
                                nl::json::parse(py::module::import("json").attr("dumps")(
                                    displayhook.attr("parent")
                                ).cast<std::string>()), {},
                                nl::json::parse(py::module::import("json").attr("dumps")(
                                    py::dict("traceback"_a = stb,
                                             "ename"_a = etype.attr("__name__"),
                                             "evalue"_a = py::str(std::move(evalue)))
                                ).cast<std::string>()), {}
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
        shell_display_hook::set_parent(self.attr("displayhook"), parent);
        display_publisher::set_parent(self.attr("display_pub"), parent);

        auto sys{py::module::import("sys")};

        zmq_ostream::set_parent(sys.attr("stdout"), parent);
        zmq_ostream::set_parent(sys.attr("stderr"), parent);
    }

    auto shell::init_environment(py::object self) -> void {
        boost::ignore_unused(self);
        auto environ{py::module::import("os").attr("environ")};

        environ["TERM"] = "xterm-color";
        environ["CLICOLOR"] = "1";
        environ["PAGER"] = "cat";
        environ["GIT_PAGER"] = "cat";
    }

    auto shell::init_virtualenv(py::object self) -> void {
        boost::ignore_unused(self);
    }

}}}
