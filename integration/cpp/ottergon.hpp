#pragma once

#include <memory>

#include <integration/cpp/base_spaces.hpp>

namespace ottergon {

    class ottergon_t final : public base_ottergon_t {
    public:
        explicit ottergon_t(const configuration::config& config)
            : base_ottergon_t(config) {}
    };

   using ottergon_ptr = std::unique_ptr<ottergon_t>;

   auto make_ottergon() -> ottergon_ptr;
   auto make_ottergon(configuration::config) -> ottergon_ptr;
   auto execute_sql(const ottergon_ptr& ottergon, const std::string& query) -> components::result::result_t;
}