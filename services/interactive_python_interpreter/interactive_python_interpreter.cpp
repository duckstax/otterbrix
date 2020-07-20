#include "interactive_python_interpreter.hpp"

namespace services {

    class zmqbuffer;
    interactive_python_interpreter::interactive_python_interpreter(
        goblin_engineer::components::root_manager* env,
        const components::python_sandbox_configuration& configuration,
        components::log_t& log)
        : goblin_engineer::abstract_service(env, "python_sandbox")
        , log_(log.clone()){
        python_interpreter_ = std::make_unique<components::python_interpreter>(configuration,log,[this](const std::string&socket_name,std::vector<std::string> msg){
          actor_zeta::send(this->address(),this->addresses(),"write",zmqbuffer(socket_name,msg));
        });

        python_interpreter_->init();
        python_interpreter_->start();

        add_handler("dispatch_shell",&interactive_python_interpreter::dispatch_shell);
        add_handler("dispatch_control",&interactive_python_interpreter::dispatch_control);

    }
    auto interactive_python_interpreter::registration(std::vector<std::string>) -> void {
    }
    auto interactive_python_interpreter::dispatch_shell(std::vector<std::string> msgs) -> void {
    }
    auto interactive_python_interpreter::dispatch_control(std::vector<std::string> msgs) -> void {
    }


} // namespace services
