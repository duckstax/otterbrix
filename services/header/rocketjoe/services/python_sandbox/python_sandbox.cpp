#include <rocketjoe/services/python_sandbox/python_sandbox.hpp>

#include <iostream>

#include <boost/filesystem.hpp>

#include <goblin-engineer.hpp>
#include <actor-zeta/core.hpp>

#include <pybind11/stl.h>
#include <pybind11/pybind11.h>

#include <rocketjoe/network/network.hpp>
#include <rocketjoe/services/python_sandbox/detail/context_manager.hpp>
#include <rocketjoe/services/python_sandbox/detail/context.hpp>
#include <rocketjoe/services/python_sandbox/detail/file_manager.hpp>
#include <rocketjoe/services/python_sandbox/detail/celery.hpp>
#include <rocketjoe/services/python_sandbox/detail/file_system.hpp>
#include <rocketjoe/services/python_sandbox/detail/data_set.hpp>

namespace rocketjoe { namespace services {

        using namespace py::literals;

        python_sandbox_t::python_sandbox_t(network::server *ptr, goblin_engineer::dynamic_config &configuration)
                : abstract_service(ptr, "python_sandbox"), python_{}, pyrocketjoe{"pyrocketjoe"},
                  file_manager_(std::make_unique<python_sandbox::detail::file_manager>()),
                  context_manager_(std::make_unique<python_sandbox::detail::context_manager>(*file_manager_)) {

///            add_handler(
///                    "dispatcher",
///                    [](actor_zeta::actor::context &, ::rocketjoe::network::query_context &) -> void {
///                        std::cerr << "Warning" << std::endl;
///                    }
///            );

///            add_handler(
///                    "write",
///                    [](actor_zeta::actor::context &ctx) -> void {
///                        actor_zeta::send(ctx->addresses("http"), std::move(ctx.message()));
///                    }
///            );


            std::cerr << "processing env python start " << std::endl;

            path_script = configuration.as_object().at("app").as_string();

            python_sandbox::detail::add_file_system(pyrocketjoe, file_manager_.get());

            ///python_sandbox::detail::add_mapreduce(pyrocketjoe, context_manager_.get());

            python_sandbox::detail::add_celery(pyrocketjoe);

            std::cerr << "processing env python finish " << std::endl;

            start();

        }

        constexpr static char init_script[] = R"__(
            import sys, os
            from importlib import import_module

            class Worker(pyrocketjoe.celery.apps.Worker):
                def __init__(self, app) -> None:
                    super().__init__(app)

            pyrocketjoe.celery.apps.Worker = Worker

            sys.modules['pyrocketjoe'] = pyrocketjoe
            sys.path.insert(0, os.path.dirname(path))

            module_name, _ = os.path.splitext(path)
            import_module(os.path.basename(module_name))

        )__";

        auto python_sandbox_t::start() -> void {
            if (path_script.extension() == ".py") {
                exuctor = std::make_unique<std::thread>(
                        [this]() {
                            auto locals = py::dict(
                                    "path"_a = path_script.string(),
                                    "pyrocketjoe"_a = pyrocketjoe
                            );

                            py::exec(init_script, py::globals(), locals);
                        }
                );
            }
        }

}}
