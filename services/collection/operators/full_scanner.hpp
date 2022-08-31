#pragma once

#include "scanner.hpp"
#include <services/collection/operators/predicates/predicate.hpp>

namespace services::collection::operators {

    class full_scanner final : public scanner {
    public:
        full_scanner(const context_t &context, collection_t* collection);

    private:
        result_find scan_impl(components::ql::find_statement& cond) final;
        result_find scan_one_impl(components::ql::find_statement& cond) final;
    };

} // namespace services::operators