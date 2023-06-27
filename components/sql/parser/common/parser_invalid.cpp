#include "parser_invalid.hpp"

namespace components::sql::invalid {

    bool parse(std::string_view, ql::variant_statement_t& statement) {
        statement = ql::unused_statement_t{};
        return true;
    }

} // namespace components::sql::invalid
