#include <rocketjoe/services/python_engine/python_sandbox.hpp>

#include <boost/filesystem.hpp>
#include <pybind11/stl.h>

#include <goblin-engineer/dynamic.hpp>
#include <rocketjoe/services/python_engine/detail/file_system.hpp>
#include <rocketjoe/services/python_engine/detail/mapreduce.hpp>
#include <rocketjoe/services/python_engine/detail/celery.hpp>
#include <rocketjoe/services/python_engine/detail/mapreduce_context.hpp>


namespace rocketjoe { namespace services { namespace python_engine {

    using namespace pybind11::literals;

    auto python_context::run() -> void {
        if(path_script.extension() == ".py") {
            exuctor = std::make_unique<std::thread>(
                    [this]() {
                        auto locals = py::dict(
                              "path"_a=path_script.string(),
                              "pyrocketjoe"_a=pyrocketjoe
                        );

                        py::exec(R"(
                           import sys, os
                           from importlib import import_module

                           sys.modules['pyrocketjoe'] = pyrocketjoe
                           sys.path.insert(0, os.path.dirname(path))

                           module_name, _ = os.path.splitext(path)

                           import_module(os.path.basename(module_name))

                        )", py::globals(), locals);
                    }
            );
        }
    }


    python_context::python_context(
            goblin_engineer::dynamic_config &configuration,
            actor_zeta::actor::actor_address ptr)
            : address(std::move(ptr))
            , python_{}
            , pyrocketjoe{"pyrocketjoe"}
            , file_manager_(std::make_unique<detail::file_manager>())
            , context_manager_(std::make_unique<detail::context_manager>(*file_manager_))
            {

        std::cerr << "processing env python start " << std::endl;

        path_script = configuration.as_object().at("app").as_string();

        add_file_system(pyrocketjoe,file_manager_.get());

        detail::add_mapreduce(pyrocketjoe,context_manager_.get());

        detail::add_celery(pyrocketjoe);

        std::cerr << "processing env python finish " << std::endl;

    }

}}}