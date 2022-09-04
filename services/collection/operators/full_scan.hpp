#pragma once

#include "operator.hpp"
#include <services/collection/operators/predicates/predicate.hpp>

namespace services::collection::operators {

    class full_scan final : public operator_t {
    public:
        full_scan(collection_t* collection);

    private:
        result_find scan_impl(components::ql::find_statement& cond) final;
        result_find scan_one_impl(components::ql::find_statement& cond) final;
    };

} // namespace services::operators