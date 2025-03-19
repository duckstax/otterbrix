#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_update.hpp>
#include <variant>

static const database_name_t database_name = "testdatabase";
static const collection_name_t collection_name = "testcollection";
static const collection_name_t collection_left = "testcollection_left_join";
static const collection_name_t collection_right = "testcollection_right_join";

using namespace components;
using namespace components::cursor;
using expressions::compare_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

static constexpr int kNumInserts = 100;

TEST_CASE("integration::cpp::test_collection::logical_plan") {
    auto config = test_create_config("/tmp/test_collection_logical_plan");
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
        auto ins = logical_plan::make_node_insert(dispatcher->resource(), {database_name, collection_name}, documents);
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(session, ins);
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
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(), {database_name, collection_name});
            auto cur = dispatcher->execute_plan(session, agg);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        }
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(), {database_name, collection_name});
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::gt,
                                                                         key{"count"},
                                                                         id_par{1});
            agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                            {database_name, collection_name},
                                                            std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, new_value(90));
            auto cur = dispatcher->execute_plan(session, agg, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }
    }

    INFO("delete") {
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(), {database_name, collection_name});
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::gt,
                                                                         key{"count"},
                                                                         id_par{1});
            agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                            {database_name, collection_name},
                                                            std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, new_value(90));
            auto cur = dispatcher->execute_plan(session, agg, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }
        {
            auto session = otterbrix::session_id_t();
            auto del = logical_plan::make_node_delete_many(
                dispatcher->resource(),
                {database_name, collection_name},
                logical_plan::make_node_match(
                    dispatcher->resource(),
                    {database_name, collection_name},
                    make_compare_expression(dispatcher->resource(), compare_type::gt, key{"count"}, id_par{1})));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, new_value(90));
            auto cur = dispatcher->execute_plan(session, del, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(), {database_name, collection_name});
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::gt,
                                                                         key{"count"},
                                                                         id_par{1});
            agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                            {database_name, collection_name},
                                                            std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, new_value(90));
            auto cur = dispatcher->execute_plan(session, agg, params);
            REQUIRE_FALSE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
    }

    INFO("update") {
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(), {database_name, collection_name});
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::lt,
                                                                         key{"count"},
                                                                         id_par{1});
            agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                            {database_name, collection_name},
                                                            std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, new_value(20));
            auto cur = dispatcher->execute_plan(session, agg, params);
            REQUIRE(cur->size() == 20);
        }
        {
            auto session = otterbrix::session_id_t();
            auto match = logical_plan::make_node_match(
                dispatcher->resource(),
                {database_name, collection_name},
                make_compare_expression(dispatcher->resource(), compare_type::lt, key{"count"}, id_par{1}));
            auto doc = make_document(dispatcher->resource());
            doc->set_dict("$set");
            doc->get_dict("$set")->set("count", 1000);
            auto upd = make_node_update_many(dispatcher->resource(), {database_name, collection_name}, match, doc);
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, new_value(20));
            auto cur = dispatcher->execute_plan(session, upd, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 20);
        }
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(), {database_name, collection_name});
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::lt,
                                                                         key{"count"},
                                                                         id_par{1});
            agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                            {database_name, collection_name},
                                                            std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, new_value(20));
            auto cur = dispatcher->execute_plan(session, agg, params);
            REQUIRE_FALSE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(), {database_name, collection_name});
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::eq,
                                                                         key{"count"},
                                                                         id_par{1});
            agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                            {database_name, collection_name},
                                                            std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, new_value(1000));
            auto cur = dispatcher->execute_plan(session, agg, params);
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
            auto ins_left = logical_plan::make_node_insert(dispatcher->resource(),
                                                           {database_name, collection_left},
                                                           documents_left);
            auto cur = dispatcher->execute_plan(session, ins_left);
        }
        {
            auto session = otterbrix::session_id_t();
            auto ins_right = logical_plan::make_node_insert(dispatcher->resource(),
                                                            {database_name, collection_right},
                                                            documents_right);
            auto cur = dispatcher->execute_plan(session, ins_right);
        }
        INFO("right is raw data") {
            auto session = otterbrix::session_id_t();
            auto join = logical_plan::make_node_join(dispatcher->resource(), {}, logical_plan::join_type::inner);
            join->append_child(
                logical_plan::make_node_aggregate(dispatcher->resource(), {database_name, collection_left}));
            join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), documents_right));
            {
                join->append_expression(expressions::make_compare_expression(dispatcher->resource(),
                                                                             compare_type::eq,
                                                                             expressions::key_t{"key_1"},
                                                                             expressions::key_t{"key"}));
            }
            auto cur = dispatcher->execute_plan(session, join);
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
            auto join = logical_plan::make_node_join(dispatcher->resource(), {}, logical_plan::join_type::inner);
            join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), documents_left));
            join->append_child(
                logical_plan::make_node_aggregate(dispatcher->resource(), {database_name, collection_right}));
            {
                join->append_expression(expressions::make_compare_expression(dispatcher->resource(),
                                                                             compare_type::eq,
                                                                             expressions::key_t{"key_1"},
                                                                             expressions::key_t{"key"}));
            }
            auto cur = dispatcher->execute_plan(session, join);
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
            auto join = logical_plan::make_node_join(dispatcher->resource(), {}, logical_plan::join_type::inner);
            join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), documents_left));
            join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), documents_right));
            {
                join->append_expression(expressions::make_compare_expression(dispatcher->resource(),
                                                                             compare_type::eq,
                                                                             expressions::key_t{"key_1"},
                                                                             expressions::key_t{"key"}));
            }
            auto cur = dispatcher->execute_plan(session, join);
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
            auto aggregate = logical_plan::make_node_aggregate(dispatcher->resource(), {});
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            {
                {
                    std::vector<expressions::expression_ptr> sort = {
                        expressions::make_sort_expression(key("avg"), expressions::sort_order::desc)};
                    aggregate->append_child(logical_plan::make_node_sort(dispatcher->resource(), {}, std::move(sort)));
                }
                {
                    // test data does not have any overlaping values, so group here is for raw_data support
                    // not for a functionality
                    auto group = logical_plan::make_node_group(dispatcher->resource(), {});
                    auto scalar_expr =
                        make_scalar_expression(dispatcher->resource(), expressions::scalar_type::get_field, key("_id"));
                    scalar_expr->append_param(key("key_1"));
                    group->append_expression(std::move(scalar_expr));

                    auto count_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                             expressions::aggregate_type::count,
                                                                             key("count"));
                    count_expr->append_param(key("name"));
                    group->append_expression(std::move(count_expr));

                    auto sum_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                           expressions::aggregate_type::sum,
                                                                           key("sum"));
                    sum_expr->append_param(key("value"));
                    group->append_expression(std::move(sum_expr));

                    auto avg_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                           expressions::aggregate_type::avg,
                                                                           key("avg"));
                    avg_expr->append_param(key("key"));
                    group->append_expression(std::move(avg_expr));

                    auto min_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                           expressions::aggregate_type::min,
                                                                           key("min"));
                    min_expr->append_param(key("value"));
                    group->append_expression(std::move(min_expr));

                    auto max_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                           expressions::aggregate_type::max,
                                                                           key("max"));
                    max_expr->append_param(key("value"));
                    group->append_expression(std::move(max_expr));

                    aggregate->append_child(std::move(group));
                }
                {
                    aggregate->append_child(
                        logical_plan::make_node_match(dispatcher->resource(),
                                                      {},
                                                      make_compare_expression(dispatcher->resource(),
                                                                              compare_type::lt,
                                                                              key("key_1"),
                                                                              core::parameter_id_t(1))));
                }
                params->add_parameter(core::parameter_id_t(1), new_value(int64_t{75}));
            }
            {
                auto join = logical_plan::make_node_join(dispatcher->resource(), {}, logical_plan::join_type::inner);
                join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), documents_left));
                join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), documents_right));
                {
                    join->append_expression(expressions::make_compare_expression(dispatcher->resource(),
                                                                                 compare_type::eq,
                                                                                 expressions::key_t{"key_1"},
                                                                                 expressions::key_t{"key"}));
                }
                aggregate->append_child(join);
            }
            auto cur = dispatcher->execute_plan(session, aggregate, params);
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
        INFO("just raw data") {
            auto session = otterbrix::session_id_t();
            auto node = logical_plan::make_node_raw_data(dispatcher->resource(), documents_left);
            auto cur = dispatcher->execute_plan(session, node);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == documents_left.size());

            for (int num = 0; num < cur->size(); ++num) {
                REQUIRE(cur->has_next());
                cur->next();
                REQUIRE(cur->get() == documents_left[num]);
            }
        }
    }
}
