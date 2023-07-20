#include "parser_update.hpp"
#include "impl/parser_update_base.hpp"

#define PARSE(F) if (!ok) ok = impl::F(resource, query, statement)

namespace components::sql::update {

    components::sql::impl::parser_result parse(std::pmr::memory_resource* resource,
                                               std::string_view query,
                                               ql::variant_statement_t& statement) {
        components::sql::impl::parser_result ok{false};
        PARSE(parse_update_base);
        return ok;
    }

} // namespace components::sql::update
