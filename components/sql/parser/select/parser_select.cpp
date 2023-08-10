#include "parser_select.hpp"
#include "impl/parser_select_from.hpp"
#include "impl/parser_select_without_from.hpp"

#define PARSE(F) if (!ok) ok = impl::F(resource, query, statement)

namespace components::sql::select {

    components::sql::impl::parser_result parse(std::pmr::memory_resource* resource,
                                               std::string_view query,
                                               ql::variant_statement_t& statement) {
        components::sql::impl::parser_result ok{false};
        PARSE(parse_select_from);
        PARSE(parse_select_without_from);
        return ok;
    }

} // namespace components::sql::select
