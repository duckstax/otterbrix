#include <catch2/catch.hpp>
#include <components/tests/generaty.hpp>
#include <services/collection/operators/get/simple_value.hpp>

using namespace components;
using namespace services::collection::operators;

TEST_CASE("operator::get::get_value") {
    auto doc = gen_doc(1);

    auto getter = get::simple_value_t::create(ql::key_t("count"));
    REQUIRE(getter->value(doc));
    REQUIRE(getter->value(doc)->as_int() == 1);

    getter = get::simple_value_t::create(ql::key_t("countStr"));
    REQUIRE(getter->value(doc));
    REQUIRE(getter->value(doc)->as_string() == "1");

    getter = get::simple_value_t::create(ql::key_t("countArray.0"));
    REQUIRE(getter->value(doc));
    REQUIRE(getter->value(doc)->as_int() == 1);

    getter = get::simple_value_t::create(ql::key_t("countDict.even"));
    REQUIRE(getter->value(doc));
    REQUIRE(getter->value(doc)->as_bool() == false);

    getter = get::simple_value_t::create(ql::key_t("no_valid"));
    REQUIRE_FALSE(getter->value(doc));

    getter = get::simple_value_t::create(ql::key_t("countArray.10"));
    REQUIRE_FALSE(getter->value(doc));

    getter = get::simple_value_t::create(ql::key_t("countDict.no_valid"));
    REQUIRE_FALSE(getter->value(doc));
}
