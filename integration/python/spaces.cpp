#include "spaces.hpp"

namespace otterbrix {

    spaces* spaces::get_instance() {
        static spaces instance;
        return &instance;
    }

    spaces::spaces()
        : base_otterbrix_t(configuration::config::default_config()) {
    }

} // namespace python