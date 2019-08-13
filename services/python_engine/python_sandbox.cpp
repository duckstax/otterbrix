#include <rocketjoe/services/python_engine/python_sandbox.hpp>

#include <boost/filesystem.hpp>
#include <pybind11/stl.h>

#include <goblin-engineer/dynamic.hpp>
#include <rocketjoe/services/python_engine/utils.hpp>
#include <rocketjoe/services/python_engine/mapreduce.hpp>
#include "rocketjoe/services/python_engine/data_set_manager.hpp"
#include "rocketjoe/services/python_engine/celery.hpp"


namespace rocketjoe { namespace services { namespace python_engine {

    using namespace pybind11::literals;

    auto python_context::run() -> void {
        auto ext = boost::filesystem::extension(path_script);

        if(ext == ".py") {
            exuctor = std::make_unique<std::thread>(
                    [this]() {
                      auto locals = py::dict(
                              "path"_a=path_script,
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
            : python{}
            , pyrocketjoe{"pyrocketjoe"}
            , address(std::move(ptr))
            , file_manager_(std::make_unique<file_manager>())
            , data_set_manager_(std::make_unique<data_set_manager>(file_manager_.get()))
            {

        std::cerr << "processing env python start " << std::endl;

        path_script = configuration.as_object().at("app").as_string();

        std::cerr << "processing env python finish " << std::endl;

        add_file_read(pyrocketjoe,file_manager_.get());

        add_mapreduce(pyrocketjoe,data_set_manager_.get());

        add_celery(pyrocketjoe);

    }

}}}