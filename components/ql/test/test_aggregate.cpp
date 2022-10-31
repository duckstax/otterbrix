#include <catch2/catch.hpp>

#include <components/ql/aggregate.hpp>
#include <components/ql/aggregate/match.hpp>

using namespace components::ql;
using namespace components::ql::aggregate;

TEST_CASE("aggregate::match") {
    auto match = make_match(experimental::make_expr(condition_type::eq, "key", core::parameter_id_t(1)));
    REQUIRE(debug(match) == R"($match: {"key": {"$eq": #1}})");
}