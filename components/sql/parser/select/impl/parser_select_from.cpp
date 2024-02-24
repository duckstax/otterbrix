#include "parser_select_from.hpp"
#include <components/sql/parser/base/parser_groupby.hpp>
#include <components/sql/parser/base/parser_mask.hpp>
#include <components/sql/parser/base/parser_orderby.hpp>
#include <components/sql/parser/base/parser_where.hpp>
#include <components/sql/parser/select/impl/parser_select_fields.hpp>

using namespace components::sql::impl;

namespace components::sql::select::impl {

<<<<<<< HEAD
=======

>>>>>>> 2dbe892... Working join operator
    components::sql::impl::parser_result parse_join_type(components::sql::lexer_t& lexer, ql::join_ptr& join) {
        static const mask_element_t mask_elem_join(token_type::bare_word, "join");
        
        static const mask_group_element_t mask_inner_join({"inner", "join"});
        static const mask_group_element_t mask_full_outer_join({"full", "outer", "join"});
        static const mask_group_element_t mask_left_outer_join({"left", "outer", "join"});
        static const mask_group_element_t mask_right_outer_join({"right", "outer", "join"});
        static const mask_group_element_t mask_cross_join({"cross", "join"});

<<<<<<< HEAD
        static const mask_group_element_t mask_inner_join({"inner", "join"});
        static const mask_group_element_t mask_full_outer_join({"full", "outer", "join"});
        static const mask_group_element_t mask_left_outer_join({"left", "outer", "join"});
        static const mask_group_element_t mask_right_outer_join({"right", "outer", "join"});
        static const mask_group_element_t mask_cross_join({"cross", "join"});

=======
>>>>>>> 2dbe892... Working join operator
        lexer.save();
        auto token = lexer.current_significant_token();
        if (mask_elem_join == token) {
            join = ql::make_join(ql::join_type::inner);
            return true;
        }
        lexer.restore();

        lexer.save();
        auto status_order = mask_inner_join.check(lexer);
        if (status_order == mask_group_element_t::status::yes) {
            join = ql::make_join(ql::join_type::inner);
            return true;
        } else if (status_order == mask_group_element_t::status::error) {
            return parser_result{parse_error::syntax_error, token, "not valid select query"};
        }
        lexer.restore();

        lexer.save();
        status_order = mask_full_outer_join.check(lexer);
        if (status_order == mask_group_element_t::status::yes) {
            join = ql::make_join(ql::join_type::full);
            return true;
        } else if (status_order == mask_group_element_t::status::error) {
            return parser_result{parse_error::syntax_error, token, "not valid select query"};
        }
        lexer.restore();

        lexer.save();
        status_order = mask_left_outer_join.check(lexer);
        if (status_order == mask_group_element_t::status::yes) {
            join = ql::make_join(ql::join_type::left);
            return true;
        } else if (status_order == mask_group_element_t::status::error) {
            return parser_result{parse_error::syntax_error, token, "not valid select query"};
        }
        lexer.restore();

        lexer.save();
        status_order = mask_right_outer_join.check(lexer);
        if (status_order == mask_group_element_t::status::yes) {
            join = ql::make_join(ql::join_type::right);
            return true;
        } else if (status_order == mask_group_element_t::status::error) {
            return parser_result{parse_error::syntax_error, token, "not valid select query"};
        }
        lexer.restore();

        lexer.save();
        status_order = mask_cross_join.check(lexer);
        if (status_order == mask_group_element_t::status::yes) {
            join = ql::make_join(ql::join_type::cross);
            return true;
        } else if (status_order == mask_group_element_t::status::error) {
            return parser_result{parse_error::syntax_error, token, "not valid select query"};
        }
        lexer.restore();
        return true;
    }

    components::sql::impl::parser_result parse_table_name(components::sql::lexer_t& lexer, ql::ql_statement_ptr ql) {
        auto token = lexer.current_significant_token();
        if (token.type == token_type::bare_word) {
            ql->collection_ = std::string(token.value());
            token = lexer.next_token();
            if (token.type == token_type::dot) {
                token = lexer.next_token();
                if (token.type != token_type::bare_word) {
                    return components::sql::impl::parser_result{parse_error::syntax_error,
                                                                token,
                                                                "not valid select query"};
                }
                ql->database_ = ql->collection_;
                ql->collection_ = std::string(token.value());
            }
        } else {
            return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid select query"};
        }
        return true;
    }

    components::sql::impl::parser_result
    parse_select_from(std::pmr::memory_resource* resource, std::string_view query, ql::variant_statement_t& statement) {
        static const mask_element_t mask_elem_select(token_type::bare_word, "select");
        static const mask_element_t mask_elem_from(token_type::bare_word, "from");
        static const mask_element_t mask_elem_on(token_type::bare_word, "on");
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

        auto agg = ql::make_aggregate("", "");
        token = lexer.next_not_whitespace_token();

        // fields
        ql::aggregate::group_t group;
        std::pmr::set<token_t> group_fields_select(resource);
        auto res = parse_select_fields(resource, lexer, group, *agg, group_fields_select);
        if (res.is_error()) {
            return res;
        }

        token = lexer.next_not_whitespace_token();
        res = parse_table_name(lexer, agg);
        if (res.is_error()) {
            return res;
        }

        // join
        ql::join_ptr join = nullptr;
        token = lexer.next_not_whitespace_token();
        while (true) {
            token = lexer.current_significant_token();
            ql::join_ptr sub_join = nullptr;
            res = parse_join_type(lexer, sub_join);
            if (res.is_error()) {
                return res;
            }
            if (sub_join) {
                sub_join->left =
                    join ? static_cast<ql::ql_statement_ptr>(join) : static_cast<ql::ql_statement_ptr>(agg);
                sub_join->right = ql::make_aggregate("", "");
                lexer.next_not_whitespace_token();
                res = parse_table_name(lexer, sub_join->right);
                if (res.is_error()) {
                    return res;
                }
                join = sub_join;
                token = lexer.next_not_whitespace_token();
                if (mask_elem_on != token) {
                    return components::sql::impl::parser_result{parse_error::syntax_error,
                                                                token,
                                                                "not valid select query"};
                }
                token = lexer.next_not_whitespace_token();
                res = parse_join_on(resource, lexer, *join);
                if (res.is_error()) {
                    return res;
                }
            } else {
                break;
            }
        }

        // where
        token = lexer.current_significant_token();
        if (mask_elem_where == token) {
            auto match = ql::aggregate::make_match(nullptr);
            auto res = parse_where(resource, lexer, match, *agg);
            if (res.is_error()) {
                return res;
            }
            agg->append(ql::aggregate::operator_type::match, match);
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
            agg->append(ql::aggregate::operator_type::group, group);
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
            agg->append(ql::aggregate::operator_type::sort, sort);
        }

        token = lexer.current_significant_token();
        if (!is_token_end_query(token)) {
            statement = ql::unused_statement_t{};
            return components::sql::impl::parser_result{parse_error::syntax_error, token, "not valid select query"};
        }

        if (join) {
            statement = std::move(*join.detach());
        } else {
            statement = std::move(*agg.detach());
        }
        return true;
    }

} // namespace components::sql::select::impl