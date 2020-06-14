#include "interactive_python_interpreter.hpp"

namespace services {

    interactive_python_interpreter::interactive_python_interpreter(
        goblin_engineer::components::root_manager* env,
        const components::python_sandbox_configuration& configuration,
        components::log_t& log)
        : goblin_engineer::abstract_manager_service(env, "python_sandbox")
        , log_(log.clone())
        , python_interpreter_(configuration, log) {
    }

    void interactive_python_interpreter::enqueue(goblin_engineer::message, actor_zeta::executor::execution_device*) {}

    auto interactive_python_interpreter::start() -> void {
        python_interpreter_.start();
    }
    auto interactive_python_interpreter::init() -> void {
        python_interpreter_.init();
    }

} // namespace services
