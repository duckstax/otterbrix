#include "parser_select_without_from.hpp"
#include <components/sql/parser/base/parser_mask.hpp>
#include <components/sql/parser/select/impl/parser_select_fields.hpp>

using namespace components::sql::impl;

namespace components::sql::select::impl {

    components::sql::impl::parser_result parse_select_without_from(std::pmr::memory_resource* resource,
                                                                   std::string_view query,
                                                                   ql::variant_statement_t& statement) {
        static const mask_element_t mask_elem_select(token_type::bare_word, "select");

        lexer_t lexer(query);

        auto token = lexer.next_not_whitespace_token();
        if (mask_elem_select != token) {
            return false;
        }

        ql::aggregate_statement agg{"", ""};
        token = lexer.next_not_whitespace_token();

        // fields
        ql::aggregate::group_t group;
        std::pmr::set<token_t> group_fields_select(resource);
        auto res = parse_select_fields(resource, lexer, group, agg, group_fields_select);
        if (res.is_error()) {
            return res;
        }

        if (!group.fields.empty()) {
            agg.append(ql::aggregate::operator_type::group, group);
        }

        statement = std::move(agg);
        return true;
    }

} // namespace components::sql::select::impl
