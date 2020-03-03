#pragma once

#include <string>
#include <thread>

#include <boost/filesystem.hpp>

#include <pybind11/embed.h>

#include <goblin-engineer/abstract_service.hpp>

#include <rocketjoe/http_server/server.hpp>
#include <rocketjoe/jupyter/interpreter.hpp>
#include <rocketjoe/python_sandbox/detail/forward.hpp>

namespace rocketjoe { namespace services {
    namespace py = pybind11;

    enum class sandbox_mode : std::uint8_t {
        none = 0,
        script = 1,
        jupyter = 2,
    };

    class BOOST_SYMBOL_VISIBLE python_sandbox_t final : public goblin_engineer::abstract_service {
    public:
        python_sandbox_t(network::server*, goblin_engineer::dynamic_config &);

        ~python_sandbox_t() override = default;

    private:
        auto jupyter_kernel_init() -> void;

        auto start() -> void;

        boost::filesystem::path jupyter_connection_path;
        boost::filesystem::path script_path;
        sandbox_mode mode;
        py::scoped_interpreter python_;
        py::module pyrocketjoe;
        std::unique_ptr<python_sandbox::detail::file_manager> file_manager_;
        std::unique_ptr<python_sandbox::detail::context_manager> context_manager_;
        std::unique_ptr<zmq::context_t> zmq_context;
        std::vector<zmq::pollitem_t> jupyter_kernel_polls;
        std::unique_ptr<interpreter> jupyter_kernel;
        std::unique_ptr<std::thread> exuctor;  ///TODO: HACK
    };

}}
