#pragma once

#include <goblin-engineer/abstract_service.hpp>
#include <rocketjoe/services/http_server/server.hpp>

namespace rocketjoe { namespace services {

    namespace python_engine {
        class python_context;
    }


    class python_vm final : public goblin_engineer::abstract_service {
    public:
        python_vm(network::server*,goblin_engineer::dynamic_config &);

        ~python_vm() override = default;

    private:
        std::unique_ptr<python_engine::python_context> pimpl;
    };

}}
