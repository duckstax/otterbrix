#pragma once

#include <goblin-engineer/abstract_service.hpp>

namespace rocketjoe { namespace services {

            class control_block final : public goblin_engineer::abstract_service {
            public:
                control_block(goblin_engineer::dynamic_config&, goblin_engineer::abstract_environment * );

                ~control_block() override = default;

                void startup(goblin_engineer::context_t *) override;

                void shutdown() override;
            private:
                class impl;
                std::unique_ptr<impl> pimpl;
            };

}}