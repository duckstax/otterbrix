#pragma once

#include <string>
#include <thread>

#include <boost/filesystem.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <pybind11/embed.h>

#include "detail/forward.hpp"
#include "detail/jupyter/pykernel.hpp"

#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <components/python_sandbox/detail/forward.hpp>
#include <components/python_sandbox/detail/jupyter/pykernel.hpp>

namespace components {

    namespace py = pybind11;
    using detail::jupyter::pykernel;

    class BOOST_SYMBOL_VISIBLE python_interpreter final {
    public:
        python_interpreter() = delete;
        python_interpreter(const python_interpreter&) = delete;
        python_interpreter& operator=(const python_interpreter&) = delete;

        python_interpreter(const components::python_sandbox_configuration&, components::log_t&);

        ~python_interpreter();

        void run_script(const std::vector<std::string>&);

        auto registration(std::vector<std::string>&) -> void;

        auto registration() -> std::vector<std::string>;

        auto dispatch_shell(std::vector<std::string> msgs) -> void;

        auto dispatch_control(std::vector<std::string> msgs) -> void;

        auto init(zmq::context_t&, std::function<void(const std::string&, std::vector<std::string>)>) -> void;

        auto stop_session() -> void;

    private:
        auto start() -> void;

        auto jupyter_engine_init(std::function<void(const std::string&, std::vector<std::string>)>) -> void;

        auto jupyter_kernel_init(zmq::context_t&, std::function<void(const std::string&, std::vector<std::string>)>) -> void;

        boost::filesystem::path jupyter_connection_path_;
        boost::filesystem::path script_path_;
        components::sandbox_mode_t mode_;
        py::scoped_interpreter python_;
        py::module pyrocketjoe;
        std::unique_ptr<python_sandbox::detail::file_manager> file_manager_;
        std::unique_ptr<python_sandbox::detail::context_manager> context_manager_;
        boost::intrusive_ptr<pykernel> jupyter_kernel;
        components::log_t log_;
        bool engine_mode;
        std::unique_ptr<zmq::socket_t> stdin_socket_;
    };

} // namespace components
