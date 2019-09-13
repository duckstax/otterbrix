#pragma  once

#include <array>
#include <map>
#include <thread>

#include <pybind11/embed.h>
#include <goblin-engineer/abstract_service.hpp>


#include "file_manager.hpp"
#include "data_set_manager.hpp"
#include "context_manager.h"

namespace rocketjoe { namespace services { namespace python_engine {

    namespace py = pybind11;

    class python_context final {
    public:

        python_context(goblin_engineer::dynamic_config&,actor_zeta::actor::actor_address);

        ~python_context() = default;

        auto run() -> void;

    private:
        py::scoped_interpreter python_;
        py::module pyrocketjoe;
        boost::filesystem::path path_script;
        std::unique_ptr<file_manager> file_manager_;
        std::unique_ptr<context_manager> context_manager_;
        actor_zeta::actor::actor_address address;
        std::unique_ptr<std::thread> exuctor;  ///TODO: HACK
    };

}}}
