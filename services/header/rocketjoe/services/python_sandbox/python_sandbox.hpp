#pragma once

#include <thread>

#include <boost/filesystem.hpp>

#include <pybind11/embed.h>

#include <goblin-engineer/abstract_service.hpp>
#include <rocketjoe/services/http_server/server.hpp>

#include <rocketjoe/services/python_sandbox/detail/forward.hpp>

namespace rocketjoe { namespace services {


    namespace py = pybind11;

    class python_sandbox_t final : public goblin_engineer::abstract_service {
    public:
        python_sandbox_t(network::server*, goblin_engineer::dynamic_config &);

        ~python_sandbox_t() override = default;

    private:

        auto start() -> void;

        py::scoped_interpreter python_;
        py::module pyrocketjoe;
        boost::filesystem::path path_script;
        std::unique_ptr<python_sandbox::detail::file_manager> file_manager_;
        std::unique_ptr<python_sandbox::detail::context_manager> context_manager_;
        std::unique_ptr<std::thread> exuctor;  ///TODO: HACK
    };

}}
