#include "spaces.hpp"

namespace ottergon {

    spaces* spaces::get_instance() {
        static spaces instance;
        return &instance;
    }

    spaces::spaces()
        : base_ottergon_t(configuration::config::default_config()) {
    }

} // namespace python