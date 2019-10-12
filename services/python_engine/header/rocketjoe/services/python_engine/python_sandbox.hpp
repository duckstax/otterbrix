#pragma  once

#include <array>
#include <map>

#include <boost/filesystem.hpp>

#include <pybind11/embed.h>
#include <goblin-engineer/abstract_service.hpp>

#include <rocketjoe/network/network.hpp>

#include "file_manager.hpp"
#include "data_set_manager.hpp"

namespace rocketjoe { namespace services { namespace python_engine {

    namespace py = pybind11;

    class python_context final {
    public:

        python_context(goblin_engineer::dynamic_config&,actor_zeta::actor::actor_address);

        ~python_context() = default;

        auto run() -> void;

    private:
        actor_zeta::actor::actor_address address;
        py::scoped_interpreter python_;
        py::module pyrocketjoe;
        boost::filesystem::path path_script;
        std::unique_ptr<file_manager> file_manager_;
        std::unique_ptr<data_set_manager> data_set_manager_;
        std::unique_ptr<std::thread> exuctor;  ///TODO: HACK
    };

}}}
