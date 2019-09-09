#pragma once

#include <goblin-engineer/abstract_service.hpp>

namespace rocketjoe { namespace services {

        namespace python_engine {
            class python_context;
        }

    class python_vm final : public goblin_engineer::abstract_service {
    public:
        python_vm(goblin_engineer::dynamic_config &, goblin_engineer::abstract_environment *);

        ~python_vm() override = default;

        void startup(goblin_engineer::context_t *) override;

        void shutdown() override;

    private:
        std::unique_ptr<python_engine::python_context> pimpl;
    };

}}
