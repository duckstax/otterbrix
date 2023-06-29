#include "spaces.hpp"

namespace duck_charmer {

    spaces* spaces::get_instance() {
        static spaces instance;
        return &instance;
    }

    spaces::spaces()
        : base_spaces(configuration::config::default_config()) {
    }

} // namespace python