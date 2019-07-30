#include <rocketjoe/services/control_block/control_block.hpp>

namespace rocketjoe { namespace services {

        class control_block::impl final {
        public:
            impl()= default;
            ~impl()= default;

        private:

        };

        control_block::control_block(
                goblin_engineer::dynamic_config &,
                goblin_engineer::abstract_environment * env
        ):
        abstract_service(env,"control_block"),
        pimpl(std::make_unique<impl>()) {

            add_handler(
                    "status",
                    [this](actor_zeta::actor::context& ctx) -> void {

                    }
            );

        }

        void control_block::startup(goblin_engineer::context_t *) {

        }

        void control_block::shutdown() {

        }

}}
