#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <components/expressions/join_expression.hpp>
#include <components/ql/statements.hpp>
#include <components/ql/statements/raw_data.hpp>
#include <expressions/aggregate_expression.hpp>
#include <variant>

static const database_name_t database_name = "TestDatabase";
static const collection_name_t collection_name = "TestCollection";
static const collection_name_t collection_left = "TestCollectionLeftJoin";
static const collection_name_t collection_right = "TestCollectionRightJoin";

using namespace components;
using namespace components::cursor;
using expressions::compare_type;
using ql::aggregate::operator_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

static constexpr int kNumInserts = 100;

TEST_CASE("integration::cpp::test_collection::ql") {
    auto config = test_create_config("/tmp/test_collection_ql");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto tape = std::make_unique<impl::base_document>(dispatcher->resource());
    auto new_value = [&](auto value) { return value_t{tape.get(), value}; };

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_left);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_right);
        }
    }

    INFO("insert") {
        std::pmr::vector<components::document::document_ptr> documents(dispatcher->resource());
        for (int num = 0; num < kNumInserts; ++num) {
            documents.push_back(gen_doc(num, dispatcher->resource()));
        }
        ql::insert_many_t ins{database_name, collection_name, documents};
        {
            auto session = otterbrix::session_id_t();
            components::ql::variant_statement_t ql{ins};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->size(session, database_name, collection_name) == kNumInserts);
        }
    }

    INFO("find") {
        {
            auto session = otterbrix::session_id_t();
            components::ql::aggregate_statement agg{database_name, collection_name, dispatcher->resource()};
            components::ql::variant_statement_t ql{agg};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        }
        {
            auto session = otterbrix::session_id_t();
            components::ql::aggregate_statement agg{database_name, collection_name, dispatcher->resource()};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::gt,
                                                                         key{"count"},
                                                                         id_par{1});
            agg.append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            agg.add_parameter(id_par{1}, new_value(90));
            components::ql::variant_statement_t ql{agg};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }
    }

    INFO("delete") {
        {
            auto session = otterbrix::session_id_t();
            components::ql::aggregate_statement agg{database_name, collection_name, dispatcher->resource()};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::gt,
                                                                         key{"count"},
                                                                         id_par{1});
            agg.append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            agg.add_parameter(id_par{1}, new_value(90));
            components::ql::variant_statement_t ql{agg};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }
        {
            auto session = otterbrix::session_id_t();
            components::ql::delete_many_t del{database_name, collection_name, dispatcher->resource()};
            del.match_.query = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                                compare_type::gt,
                                                                                key{"count"},
                                                                                id_par{1});
            del.add_parameter(id_par{1}, new_value(90));
            components::ql::variant_statement_t ql{del};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }
        {
            auto session = otterbrix::session_id_t();
            components::ql::aggregate_statement agg{database_name, collection_name, dispatcher->resource()};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::gt,
                                                                         key{"count"},
                                                                         id_par{1});
            agg.append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            agg.add_parameter(id_par{1}, new_value(90));
            components::ql::variant_statement_t ql{agg};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE_FALSE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
    }

    INFO("update") {
        {
            auto session = otterbrix::session_id_t();
            components::ql::aggregate_statement agg{database_name, collection_name, dispatcher->resource()};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::lt,
                                                                         key{"count"},
                                                                         id_par{1});
            agg.append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            agg.add_parameter(id_par{1}, new_value(20));
            components::ql::variant_statement_t ql{agg};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE(cur->size() == 20);
        }
        {
            auto session = otterbrix::session_id_t();
            components::ql::update_many_t upd{database_name, collection_name, dispatcher->resource()};
            upd.match_.query = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                                compare_type::lt,
                                                                                key{"count"},
                                                                                id_par{1});
            upd.add_parameter(id_par{1}, new_value(20));
            upd.update_ = make_document(dispatcher->resource());
            upd.update_->set_dict("$set");
            upd.update_->get_dict("$set")->set("count", 1000);
            components::ql::variant_statement_t ql{upd};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 20);
        }
        {
            auto session = otterbrix::session_id_t();
            components::ql::aggregate_statement agg{database_name, collection_name, dispatcher->resource()};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::lt,
                                                                         key{"count"},
                                                                         id_par{1});
            agg.append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            agg.add_parameter(id_par{1}, new_value(20));
            components::ql::variant_statement_t ql{agg};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE_FALSE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
        {
            auto session = otterbrix::session_id_t();
            components::ql::aggregate_statement agg{database_name, collection_name, dispatcher->resource()};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::eq,
                                                                         key{"count"},
                                                                         id_par{1});
            agg.append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            agg.add_parameter(id_par{1}, new_value(1000));
            components::ql::variant_statement_t ql{agg};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 20);
        }
    }
    INFO("join with outside data") {
        std::pmr::vector<components::document::document_ptr> documents_left(dispatcher->resource());
        std::pmr::vector<components::document::document_ptr> documents_right(dispatcher->resource());
        for (int64_t num = 0, reversed = 100; num < 101; ++num, --reversed) {
            auto doc_left = make_document(dispatcher->resource());
            doc_left->set("_id", gen_id(num + 1, dispatcher->resource()));
            doc_left->set("name", "Name " + std::to_string(num));
            doc_left->set("key_1", num);
            doc_left->set("key_2", reversed);
            documents_left.emplace_back(std::move(doc_left));
        }
        for (int64_t num = 0; num < 100; ++num) {
            auto doc_right = make_document(dispatcher->resource());
            doc_right->set("_id", gen_id(num + 1001, dispatcher->resource()));
            doc_right->set("value", (num + 25) * 2 * 10);
            doc_right->set("key", (num + 25) * 2);
            documents_right.emplace_back(std::move(doc_right));
        }
        {
            auto session = otterbrix::session_id_t();
            ql::insert_many_t ins_left{database_name, collection_left, documents_left};
            components::ql::variant_statement_t ql{ins_left};
            auto cur = dispatcher->execute_ql(session, ql);
        }
        {
            auto session = otterbrix::session_id_t();
            ql::insert_many_t ins_right{database_name, collection_right, documents_right};
            components::ql::variant_statement_t ql{ins_right};
            auto cur = dispatcher->execute_ql(session, ql);
        }
        INFO("right is raw data") {
            auto session = otterbrix::session_id_t();
            ql::join_t join(dispatcher->resource());
            join.left = ql::make_aggregate(database_name, collection_left, dispatcher->resource());
            join.right = new ql::raw_data_t(documents_right);
            {
                expressions::join_expression_field left;
                expressions::join_expression_field right;

                left.expr = expressions::make_scalar_expression(dispatcher->resource(),
                                                                expressions::scalar_type::get_field,
                                                                expressions::key_t{"key_1"});
                right.expr = expressions::make_scalar_expression(dispatcher->resource(),
                                                                 expressions::scalar_type::get_field,
                                                                 expressions::key_t{"key"});
                join.expressions.emplace_back(expressions::make_join_expression(dispatcher->resource(),
                                                                                compare_type::eq,
                                                                                std::move(left),
                                                                                std::move(right)));
            }
            components::ql::variant_statement_t ql{join};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 26);

            for (int num = 0; num < 26; ++num) {
                REQUIRE(cur->has_next());
                cur->next();
                REQUIRE(cur->get()->get_long("key_1") == (num + 25) * 2);
                REQUIRE(cur->get()->get_long("key") == (num + 25) * 2);
                REQUIRE(cur->get()->get_long("value") == (num + 25) * 2 * 10);
                REQUIRE(cur->get()->get_string("name") == std::pmr::string("Name " + std::to_string((num + 25) * 2)));
            }
        }
        INFO("left is raw data") {
            auto session = otterbrix::session_id_t();
            ql::join_t join(dispatcher->resource());
            join.left = new ql::raw_data_t(documents_left);
            join.right = ql::make_aggregate(database_name, collection_right, dispatcher->resource());
            {
                expressions::join_expression_field left;
                expressions::join_expression_field right;

                left.expr = expressions::make_scalar_expression(dispatcher->resource(),
                                                                expressions::scalar_type::get_field,
                                                                expressions::key_t{"key_1"});
                right.expr = expressions::make_scalar_expression(dispatcher->resource(),
                                                                 expressions::scalar_type::get_field,
                                                                 expressions::key_t{"key"});
                join.expressions.emplace_back(expressions::make_join_expression(dispatcher->resource(),
                                                                                compare_type::eq,
                                                                                std::move(left),
                                                                                std::move(right)));
            }
            components::ql::variant_statement_t ql{join};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 26);

            for (int num = 0; num < 26; ++num) {
                REQUIRE(cur->has_next());
                cur->next();
                REQUIRE(cur->get()->get_long("key_1") == (num + 25) * 2);
                REQUIRE(cur->get()->get_long("key") == (num + 25) * 2);
                REQUIRE(cur->get()->get_long("value") == (num + 25) * 2 * 10);
                REQUIRE(cur->get()->get_string("name") == std::pmr::string("Name " + std::to_string((num + 25) * 2)));
            }
        }
        INFO("both are raw data") {
            auto session = otterbrix::session_id_t();
            ql::join_t join(dispatcher->resource());
            join.left = new ql::raw_data_t(documents_left);
            join.right = new ql::raw_data_t(documents_right);
            {
                expressions::join_expression_field left;
                expressions::join_expression_field right;

                left.expr = expressions::make_scalar_expression(dispatcher->resource(),
                                                                expressions::scalar_type::get_field,
                                                                expressions::key_t{"key_1"});
                right.expr = expressions::make_scalar_expression(dispatcher->resource(),
                                                                 expressions::scalar_type::get_field,
                                                                 expressions::key_t{"key"});
                join.expressions.emplace_back(expressions::make_join_expression(dispatcher->resource(),
                                                                                compare_type::eq,
                                                                                std::move(left),
                                                                                std::move(right)));
            }
            components::ql::variant_statement_t ql{join};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 26);

            for (int num = 0; num < 26; ++num) {
                REQUIRE(cur->has_next());
                cur->next();
                REQUIRE(cur->get()->get_long("key_1") == (num + 25) * 2);
                REQUIRE(cur->get()->get_long("key") == (num + 25) * 2);
                REQUIRE(cur->get()->get_long("value") == (num + 25) * 2 * 10);
                REQUIRE(cur->get()->get_string("name") == std::pmr::string("Name " + std::to_string((num + 25) * 2)));
            }
        }
        INFO("join raw data with aggregate") {
            auto session = otterbrix::session_id_t();
            ql::aggregate_statement aggregate(dispatcher->resource());
            {
                {
                    ql::aggregate::sort_t sort;
                    append_sort(sort, key("avg"), expressions::sort_order::desc);
                    aggregate.append(operator_type::sort, std::move(sort));
                }
                {
                    // test data does not have any overlaping values, so group here is for raw_data support
                    // not for a functionality
                    ql::aggregate::group_t group;

                    auto scalar_expr =
                        make_scalar_expression(dispatcher->resource(), expressions::scalar_type::get_field, key("_id"));
                    scalar_expr->append_param(key("key_1"));
                    append_expr(group, std::move(scalar_expr));

                    auto count_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                             expressions::aggregate_type::count,
                                                                             key("count"));
                    count_expr->append_param(key("name"));
                    append_expr(group, std::move(count_expr));

                    auto sum_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                           expressions::aggregate_type::sum,
                                                                           key("sum"));
                    sum_expr->append_param(key("value"));
                    append_expr(group, std::move(sum_expr));

                    auto avg_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                           expressions::aggregate_type::avg,
                                                                           key("avg"));
                    avg_expr->append_param(key("key"));
                    append_expr(group, std::move(avg_expr));

                    auto min_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                           expressions::aggregate_type::min,
                                                                           key("min"));
                    min_expr->append_param(key("value"));
                    append_expr(group, std::move(min_expr));

                    auto max_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                           expressions::aggregate_type::max,
                                                                           key("max"));
                    max_expr->append_param(key("value"));
                    append_expr(group, std::move(max_expr));

                    aggregate.append(operator_type::group, std::move(group));
                }
                {
                    aggregate.append(operator_type::match,
                                     ql::aggregate::make_match(make_compare_expression(dispatcher->resource(),
                                                                                       compare_type::lt,
                                                                                       key("key_1"),
                                                                                       core::parameter_id_t(1))));
                }
                aggregate.add_parameter(core::parameter_id_t(1), new_value(int64_t{75}));
            }
            {
                auto join = boost::intrusive_ptr{new ql::join_t{dispatcher->resource()}};
                join->left = new ql::raw_data_t(documents_left);
                join->right = new ql::raw_data_t(documents_right);
                {
                    expressions::join_expression_field left;
                    expressions::join_expression_field right;

                    left.expr = expressions::make_scalar_expression(dispatcher->resource(),
                                                                    expressions::scalar_type::get_field,
                                                                    expressions::key_t{"key_1"});
                    right.expr = expressions::make_scalar_expression(dispatcher->resource(),
                                                                     expressions::scalar_type::get_field,
                                                                     expressions::key_t{"key"});
                    join->expressions.emplace_back(expressions::make_join_expression(dispatcher->resource(),
                                                                                     compare_type::eq,
                                                                                     std::move(left),
                                                                                     std::move(right)));
                }
                aggregate.data = std::move(join);
            }
            components::ql::variant_statement_t ql{aggregate};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 13);

            for (int num = 12; num >= 0; --num) {
                REQUIRE(cur->has_next());
                cur->next();
                REQUIRE(cur->get()->get_long("count") == 1);
                REQUIRE(cur->get()->get_long("sum") == (num + 25) * 2 * 10);
                REQUIRE(cur->get()->get_long("avg") == (num + 25) * 2);
                REQUIRE(cur->get()->get_long("min") == (num + 25) * 2 * 10);
                REQUIRE(cur->get()->get_long("max") == (num + 25) * 2 * 10);
            }
        }
    }
}
