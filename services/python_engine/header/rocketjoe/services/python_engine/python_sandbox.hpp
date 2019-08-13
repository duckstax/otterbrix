#pragma  once

#include <array>
#include <map>

#include <pybind11/embed.h>
#include <goblin-engineer/abstract_service.hpp>

#include <rocketjoe/services/python_engine/device.hpp>
#include <rocketjoe/network/network.hpp>

#include "file_manager.hpp"
#include "data_set_manager.hpp"

namespace rocketjoe { namespace services { namespace python_engine {

    namespace py = pybind11;

    class python_context final {
    public:

        python_context(goblin_engineer::dynamic_config&,actor_zeta::actor::actor_address);

        ~python_context() = default;

        auto push_job(network::query_context &&) -> void;

        auto run() -> void;

    private:
        device<network::query_context,network::request_type ,network::response_type > device_;
        py::scoped_interpreter python;
        py::module pyrocketjoe;
        std::string path_script;
        std::unique_ptr<file_manager> file_manager_;
        std::unique_ptr<data_set_manager> data_set_manager_;
        actor_zeta::actor::actor_address address;
        std::unique_ptr<std::thread> exuctor;  ///TODO: HACK
    };

}}}
