#include "parser_insert.hpp"
#include "impl/parser_insert_into.hpp"

#define PARSE(F) if (!ok) ok = impl::F(resource, query, statement)

namespace components::sql::insert {

    components::sql::impl::parser_result parse(std::pmr::memory_resource* resource,
                                               std::string_view query,
                                               ql::variant_statement_t& statement) {
        components::sql::impl::parser_result ok{false};
        PARSE(parse_insert_into);
        return ok;
    }

} // namespace components::sql::insert
