#include "parser_index.hpp"
#include "impl/parser_index_create.hpp"
#include "impl/parser_index_drop.hpp"

#define PARSE(F)                                                                                                       \
    if (!ok)                                                                                                           \
    ok = impl::F(resource, query, statement)

namespace components::sql::index {

    components::sql::impl::parser_result
    parse(std::pmr::memory_resource* resource, std::string_view query, ql::variant_statement_t& statement) {
        components::sql::impl::parser_result ok{false};
        PARSE(parse_create);
        PARSE(parse_drop);
        return ok;
    }

} // namespace components::sql::index
