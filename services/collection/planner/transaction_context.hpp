#pragma once

#include <components/ql/ql_param_statement.hpp>

namespace services::collection::planner {

    class transaction_context_t {
    public:
        components::ql::storage_parameters *parameters {nullptr};

        transaction_context_t() = default;
        explicit transaction_context_t(components::ql::storage_parameters *init_parameters);
    };

}
