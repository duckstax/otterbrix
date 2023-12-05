#pragma once

#include <memory>

#include <integration/cpp/base_spaces.hpp>

namespace otterbrix {

    class otterbrix_t final : public base_otterbrix_t {
    public:
        explicit otterbrix_t(const configuration::config& config)
            : base_otterbrix_t(config) {}
    };

   using otterbrix_ptr = std::unique_ptr<otterbrix_t>;

   auto make_otterbrix() -> otterbrix_ptr;
   auto make_otterbrix(configuration::config) -> otterbrix_ptr;
   auto execute_sql(const otterbrix_ptr& otterbrix, const std::string& query) -> components::result::result_t;
}