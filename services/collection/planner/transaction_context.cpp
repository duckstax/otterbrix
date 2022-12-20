#include "transaction_context.hpp"

namespace services::collection::planner {

    transaction_context_t::transaction_context_t(components::ql::storage_parameters* init_parameters)
        : parameters(init_parameters)
    {}

} // namespace services::collection::planner
