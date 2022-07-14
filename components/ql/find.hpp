#include "expr.hpp"
#include "ql_statement.hpp"

using ::document::impl::array_t;
using ::document::impl::value_t;
using ::document::impl::value_type;

namespace components::ql {

    struct find_statement final : public ql_statement_t {
        std::vector<expr_ptr> condition_;
    };

    expr_ptr make_find_condition(condition_type type, const std::string& key, const value_t* value);
} // namespace components::ql