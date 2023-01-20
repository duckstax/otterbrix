#include "spaces.hpp"

#include <integration/cpp/route.hpp>

#include <actor-zeta.hpp>

#include <memory>

#include "core/system_command.hpp"

namespace duck_charmer {

    using services::dispatcher::manager_dispatcher_t;

    constexpr static auto name_dispatcher = "dispatcher";

    spaces* spaces::instance_ = nullptr;

    spaces* spaces::get_instance() {
        if (instance_ == nullptr) {
            instance_ = new spaces();
        }
        return instance_;
    }

    spaces::spaces()
        : base_spaces(configuration::config::default_config()) {
    }

} // namespace python