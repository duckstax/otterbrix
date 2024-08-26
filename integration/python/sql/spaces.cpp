#include "spaces.hpp"

namespace otterbrix {

    spaces* spaces::get_instance() { return new spaces(); }

    spaces* spaces::get_instance(const std::filesystem::path& path) { return new spaces(path); }

    spaces::spaces()
        : base_otterbrix_t(configuration::config::default_config()) {}

    spaces::spaces(const std::filesystem::path& path)
        : base_otterbrix_t(configuration::config::create_config(path)) {}

} // namespace otterbrix