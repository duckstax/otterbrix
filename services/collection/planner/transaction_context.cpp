#include "transaction_context.hpp"

namespace services::collection::planner {

    transaction_context_t::transaction_context_t(components::ql::storage_parameters *init_parameters)
        : parameters(init_parameters)
    {}

    transaction_context_t::transaction_context_t(components::session::session_id_t session,
                                                 components::ql::storage_parameters* init_parameters)
        : session(session)
        , parameters(init_parameters)
    {}

} // namespace services::collection::planner
