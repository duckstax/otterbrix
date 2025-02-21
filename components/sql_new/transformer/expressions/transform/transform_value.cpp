#include "sql_new/transformer/expressions/value_expression.hpp"
#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"

namespace components::sql_new::transform {
    std::unique_ptr<expressions::value_expression> transformer::transform_value(Value val) {
        switch (val.type) {
            case T_Integer:
                return std::make_unique<expressions::value_expression>(static_cast<int64_t>(val.val.ival));
            case T_String:
                return std::make_unique<expressions::value_expression>(std::string(val.val.str));
            case T_Float: {
                std::string str_val(val.val.str);
                double dbl_val;
                string_to_double(str_val.data(), str_val.size(), dbl_val);
                return std::make_unique<expressions::value_expression>(dbl_val);
            }
            case T_Null:
                return std::make_unique<expressions::value_expression>(true);
            default:
                throw std::runtime_error("Value is not implemented!");
        }
    }
} // namespace components::sql_new::transform
