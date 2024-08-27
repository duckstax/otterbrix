#include "spaces.hpp"

namespace otterbrix {

    std::shared_ptr<spaces> spaces::get_instance() { return std::shared_ptr<spaces>{new spaces()}; }

    std::shared_ptr<spaces> spaces::get_instance(const std::filesystem::path& path) {
        return std::shared_ptr<spaces>{new spaces(path)};
    }

    spaces::spaces()
        : base_otterbrix_t(configuration::config::default_config()) {}

    spaces::spaces(const std::filesystem::path& path)
        : base_otterbrix_t(configuration::config::create_config(path)) {}

} // namespace otterbrix