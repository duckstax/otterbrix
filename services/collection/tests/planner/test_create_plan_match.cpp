#include <catch2/catch.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_match.hpp>
#include <services/collection/planner/create_plan.hpp>
#include <services/collection/tests/operators/test_operator_generaty.hpp>
#include <actor-zeta.hpp>

using namespace components::logical_plan;
using namespace components::expressions;
using namespace services::collection::planner;
using key = components::expressions::key_t;

constexpr auto database_name = "database";
constexpr auto collection_name = "collection";

collection_full_name_t get_name() {
    return {database_name, collection_name};
}

TEST_CASE("create_plan::match") {
    auto collection = init_collection();
    auto* resource = actor_zeta::detail::pmr::get_default_resource();
    {
        auto match = components::ql::aggregate::make_match(nullptr);
        auto node_match = make_node_match(resource, get_name(), match);
        auto plan = create_plan(d(collection)->view(), node_match, components::ql::limit_t::unlimit());
        plan->on_execute(nullptr);
        REQUIRE(plan->output()->size() == 100);
    }
    {
        auto match = components::ql::aggregate::make_match(make_compare_expression(resource, compare_type::eq, key("key"), core::parameter_id_t(1)));
        auto node_match = make_node_match(resource, get_name(), match);
        auto plan = create_plan(d(collection)->view(), node_match, components::ql::limit_t::unlimit());
        //REQUIRE(node_match->to_string() == R"_($match: {"key": {$eq: #1}})_");
    }
}
