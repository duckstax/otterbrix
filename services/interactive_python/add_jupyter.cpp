#include <add_jupyter.hpp>

#include <pybind11/pybind11.h>

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <jupyter/display_hook.hpp>
#include <jupyter/shell_display_hook.hpp>
#include <jupyter/display_publisher.hpp>
#include <jupyter/session.hpp>
#include <jupyter/shell.hpp>
#include <jupyter/zmq_ostream.hpp>

PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

using namespace pybind11::literals;
namespace py = pybind11;
using namespace  services::interactive_python_interpreter::jupyter;

namespace services { namespace interactive_python_interpreter {

    auto add_jupyter(py::module &pyrocketjoe) -> void  {
        auto pykernel = pyrocketjoe.def_submodule("pykernel");
        auto type{py::reinterpret_borrow<py::object>((PyObject *)&PyType_Type)};
        auto traitlets{py::module::import("traitlets")};
        auto tl_type{traitlets.attr("Type")};
        auto tl_any{traitlets.attr("Any")};
        auto tl_cbool{traitlets.attr("CBool")};
        auto tl_instance{traitlets.attr("Instance")};
        auto tl_default{traitlets.attr("default")};
        auto text_io_base{py::module::import("io").attr("TextIOBase")};
        auto shell_display_hook{py::module::import("IPython.core.displayhook")
                                .attr("DisplayHook")};
        auto display_publisher{py::module::import("IPython.core.displaypub")
                                   .attr("DisplayPublisher")};
        auto shell_module{py::module::import("IPython.core.interactiveshell")};
        auto shell_abc{shell_module.attr("InteractiveShellABC")};
        auto shell{shell_module.attr("InteractiveShell")};
        auto zmq_exit_autocall{py::module::import("IPython.core.autocall")
                                   .attr("ZMQExitAutocall")};
        auto zmq_ostream_props{py::dict(
            "encoding"_a = "UTF-8",
            "writable"_a = py::cpp_function(&zmq_ostream::writable,
                                            py::is_method(py::none())),
            "write"_a = py::cpp_function(&zmq_ostream::write,
                                         py::is_method(py::none())),
            "flush"_a = py::cpp_function(&zmq_ostream::flush,
                                         py::is_method(py::none()))
        )};
        auto zmq_ostream{type("ZMQOstream",
                              py::make_tuple(std::move(text_io_base)),
                              std::move(zmq_ostream_props))};
        auto shell_display_hook_props{py::dict(
            "parent"_a = tl_any("allow_none"_a = true),
            "start_displayhook"_a = py::cpp_function(
                &shell_display_hook::start_displayhook,
                py::is_method(py::none())
            ),
            "write_output_prompt"_a = py::cpp_function(
                &shell_display_hook::write_output_prompt,
                py::is_method(py::none())
            ),
            "write_format_data"_a = py::cpp_function(
                &shell_display_hook::write_format_data,
                py::is_method(py::none())
            ),
            "finish_displayhook"_a = py::cpp_function(
                &shell_display_hook::finish_displayhook,
                py::is_method(py::none())
            )
        )};
        auto rocketjoe_shell_display_hook{type(
            "RocketJoeShellDisplayHook", py::make_tuple(std::move(
                shell_display_hook
            )), std::move(shell_display_hook_props)
        )};
        auto display_publisher_props{py::dict(
            "parent"_a = tl_any("allow_none"_a = true),
            "publish"_a = py::cpp_function(&display_publisher::publish,
                                           py::is_method(py::none())),
            "clear_output"_a = py::cpp_function(
                &display_publisher::clear_output, py::is_method(py::none())
            )
        )};
        auto rocketjoe_display_publisher{type(
            "RocketJoeDisplayPublisher",
            py::make_tuple(std::move(display_publisher)),
            std::move(display_publisher_props)
        )};
        auto shell_props{py::dict(
            "displayhook_class"_a = tl_type(rocketjoe_shell_display_hook),
            "display_pub_class"_a = tl_type(rocketjoe_display_publisher),
            "readline_use"_a = tl_cbool(false),
            "autoindent"_a = tl_cbool(false),
            "exiter"_a = tl_instance(std::move(zmq_exit_autocall)),
            "keepkernel_on_exit"_a = py::none(),
            "_default_banner1"_a = tl_default("banner1")(
                py::cpp_function(&shell::_default_banner1,
                                 py::is_method(py::none()))
            ),
            "_default_exiter"_a = tl_default("exiter")(
                py::cpp_function(&shell::_default_exiter,
                                 py::is_method(py::none()))
            ),
            "init_hooks"_a = py::cpp_function(&shell::init_hooks,
                                              py::is_method(py::none())),
            "ask_exit"_a = py::cpp_function(&shell::ask_exit,
                                            py::is_method(py::none())),
            "run_cell"_a = py::cpp_function(&shell::run_cell,
                                            py::is_method(py::none())),
            "_showtraceback"_a = py::cpp_function(&shell::_showtraceback,
                                                  py::is_method(py::none())),
            "set_next_input"_a = py::cpp_function(&shell::set_next_input,
                                                  py::is_method(py::none())),
            "init_environment"_a = py::cpp_function(&shell::init_environment,
                                                    py::is_method(py::none())),
            "init_virtualenv"_a = py::cpp_function(&shell::init_virtualenv,
                                                   py::is_method(py::none()))
        )};
        auto rocketjoe_shell{type("RocketJoeShell",
                                  py::make_tuple(std::move(shell)),
                                  std::move(shell_props))};

        py::class_<socket_manager_t, boost::intrusive_ptr<socket_manager_t>>(
            pykernel, "ZMQSocket"
        );
        py::class_<display_hook>(pykernel, "RocketJoeDisplayHook")
            .def("set_execution_count", &display_hook::set_execution_count)
            .def("__call__", &display_hook::operator());
        py::class_<session, boost::intrusive_ptr<session>>(pykernel,
            "RocketJoeSession");
        pykernel.attr("ZMQOstream") = std::move(zmq_ostream);
        pykernel.attr("RocketJoeShellDisplayHook") =
            std::move(rocketjoe_shell_display_hook);
        pykernel.attr("RocketJoeDisplayPublisher") =
            std::move(rocketjoe_display_publisher);
        pykernel.attr("RocketJoeShell") = rocketjoe_shell;
        shell_abc.attr("register")(std::move(rocketjoe_shell));
    }

}}
