#include "parser_select_from.hpp"
#include <components/sql/parser/base/parser_groupby.hpp>
#include <components/sql/parser/base/parser_mask.hpp>
#include <components/sql/parser/base/parser_orderby.hpp>
#include <components/sql/parser/base/parser_where.hpp>
#include <components/sql/parser/select/impl/parser_select_fields.hpp>

using namespace components::sql::impl;

namespace components::sql::select::impl {

    components::sql::impl::parser_result
    parse_select_from(std::pmr::memory_resource* resource, std::string_view query, ql::variant_statement_t& statement) {
        static const mask_element_t mask_elem_select(token_type::bare_word, "select");
        static const mask_element_t mask_elem_from(token_type::bare_word, "from");
        static const mask_element_t mask_elem_where(token_type::bare_word, "where");
        static const mask_group_element_t mask_order_by({"order", "by"});
        static const mask_group_element_t mask_group_by({"group", "by"});

        lexer_t lexer(query);

        auto token = lexer.next_not_whitespace_token();
        if (mask_elem_select != token) {
            return false;
        }

        lexer.save();
        if (!contents_mask_element(lexer, mask_elem_from)) {
            return false;
        }
        lexer.restore();

        ql::aggregate_statement agg{"", ""};
        token = lexer.next_not_whitespace_token();

        // fields
        ql::aggregate::group_t group;
        std::pmr::set<token_t> group_fields_select(resource);
        auto res = parse_select_fields(resource, lexer, group, agg, group_fields_select);
        if (res.is_error()) {
            return res;
        }

        token = lexer.next_not_whitespace_token();
        if (token.type == token_type::bare_word) {
            agg.collection_ = std::string(token.value());
            token = lexer.next_token();
            if (token.type == token_type::dot) {
                token = lexer.next_token();
                if (token.type != token_type::bare_word) {
                    return components::sql::impl::parser_result{parse_error::syntax_error,
                                                                token,
                                                                "not valid select query"};
                }
                agg.database_ = agg.collection_;
                agg.collection_ = std::string(token.value());
            }

            // where
            token = lexer.next_not_whitespace_token();
            if (mask_elem_where == token) {
                auto match = ql::aggregate::make_match(nullptr);
                auto res = parse_where(resource, lexer, match, agg);
                if (res.is_error()) {
                    return res;
                }
                agg.append(ql::aggregate::operator_type::match, match);
            }

            // group by
            auto status_group = mask_group_by.check(lexer);
            if (status_group == mask_group_element_t::status::error) {
                return components::sql::impl::parser_result{parse_error::syntax_error,
                                                            lexer.next_not_whitespace_token(),
                                                            "invalid use group"};
            }
            std::pmr::set<token_t> group_fields(resource);
            if (status_group == mask_group_element_t::status::yes) {
                auto res = parse_groupby(lexer, group_fields);
                if (res.is_error()) {
                    return res;
                }
            }
            res = check_groupby(resource, group, group_fields_select, group_fields);
            if (res.is_error()) {
                return res;
            }
            if (!group.fields.empty()) {
                agg.append(ql::aggregate::operator_type::group, group);
            }

            // order by
            auto status_order = mask_order_by.check(lexer);
            if (status_order == mask_group_element_t::status::error) {
                return components::sql::impl::parser_result{parse_error::syntax_error,
                                                            lexer.next_not_whitespace_token(),
                                                            "invalid use order"};
            }
            if (status_order == mask_group_element_t::status::yes) {
                ql::aggregate::sort_t sort;
                auto res = parse_orderby(resource, lexer, sort);
                if (res.is_error()) {
                    return res;
                }
                agg.append(ql::aggregate::operator_type::sort, sort);
            }

            token = lexer.current_significant_token();
            if (!is_token_end_query(token)) {
                statement = ql::unused_statement_t{};
                return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid update query"};
            }

        } else {
            return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid update query"};
        }

        statement = std::move(agg);
        return true;
    }

} // namespace components::sql::select::impl
