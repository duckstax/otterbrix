#pragma once

#include <cstdint>

#include <map>
#include <variant>
#include <vector>

#include "aggregate/operator.hpp"
#include "ql/expr2.hpp"
#include "ql/ql_statement.hpp"

namespace components::ql {

    class aggregate_statement final : public ql_statement_t {
    public:
        aggregate_statement(database_name_t database, collection_name_t collection);

        ///void append(aggregate::operator_ptr ptr) {
///            aggregate_operator_.push_back(std::move(ptr));
   //     }

    private:
        aggregate::operators_t aggregate_operator_;
        std::unordered_map<core::parameter_id_t, expr_value_t> values;
    };

} // namespace components::ql