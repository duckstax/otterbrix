#include "parser.hpp"

#include "parser/base/parser_result.hpp"
#include "parser/common/parser_invalid.hpp"
#include "parser/database/parser_database.hpp"
#include "parser/insert/parser_insert.hpp"

#define PARSE(F) if (!ok) ok = F::parse(query, result)

namespace components::sql {

    ql::variant_statement_t parse(std::string_view query) {
        ql::variant_statement_t result;
        parser_result ok{false};
        PARSE(database);
        PARSE(insert);
        PARSE(invalid);
        if (ok.is_error()) {
            //todo: error
        }
        return result;
    }

    ql::variant_statement_t parse(const std::string& query) {
        return parse(std::string_view{query});
    }

    ql::variant_statement_t parse(const char* query) {
        return parse(std::string_view{query});
    }

} // namespace components::sql
