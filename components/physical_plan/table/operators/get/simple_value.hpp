#pragma once

#include "operator_get.hpp"

namespace services::table::operators::get {

    class simple_value_t : public operator_get_t {
    public:
        static operator_get_ptr create(const components::expressions::key_t& key);

    private:
        const components::expressions::key_t key_;

        explicit simple_value_t(const components::expressions::key_t& key);

        components::types::logical_value_t
        get_value_impl(const std::pmr::vector<components::types::logical_value_t>& row);
    };

} // namespace services::table::operators::get
