#pragma  once

#include <array>
#include <map>
#include <thread>

#include <boost/filesystem.hpp>

#include <pybind11/embed.h>
#include <goblin-engineer/abstract_service.hpp>

#include <rocketjoe/services/python_engine/detail/file_manager.hpp>
#include <rocketjoe/services/python_engine/detail/data_set_manager.hpp>
#include <rocketjoe/services/python_engine/detail/context_manager.hpp>

namespace rocketjoe { namespace services { namespace python_engine {

    namespace py = pybind11;

    class BOOST_SYMBOL_VISIBLE python_context final {
    public:

        python_context(goblin_engineer::dynamic_config&,actor_zeta::actor::actor_address);

        ~python_context() = default;

        auto run() -> void;

    private:
        actor_zeta::actor::actor_address address;
        py::scoped_interpreter python_;
        py::module pyrocketjoe;
        boost::filesystem::path path_script;
        std::unique_ptr<detail::file_manager> file_manager_;
        std::unique_ptr<detail::context_manager> context_manager_;
        std::unique_ptr<std::thread> exuctor;  ///TODO: HACK
    };

}}}
