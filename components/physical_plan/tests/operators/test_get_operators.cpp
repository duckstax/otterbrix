#include <catch2/catch.hpp>
#include <components/physical_plan/collection/operators/get/simple_value.hpp>
#include <components/tests/generaty.hpp>

using namespace components;
using namespace services::collection::operators;
using key = components::expressions::key_t;

TEST_CASE("operator::get::get_value") {
    auto* resource = actor_zeta::detail::pmr::get_default_resource();
    auto tape = std::make_unique<impl::base_document>(resource);
    auto doc = gen_doc(1, resource);

    auto getter = get::simple_value_t::create(key("count"));
    REQUIRE(getter->value(doc, tape.get()));
    REQUIRE(getter->value(doc, tape.get()).as_int() == 1);

    getter = get::simple_value_t::create(key("countStr"));
    REQUIRE(getter->value(doc, tape.get()));
    REQUIRE(getter->value(doc, tape.get()).as_string() == "1");

    getter = get::simple_value_t::create(key("countArray/0"));
    REQUIRE(getter->value(doc, tape.get()));
    REQUIRE(getter->value(doc, tape.get()).as_int() == 1);

    getter = get::simple_value_t::create(key("countDict/even"));
    REQUIRE(getter->value(doc, tape.get()));
    REQUIRE(getter->value(doc, tape.get()).as_bool() == false);

    getter = get::simple_value_t::create(key("invalid"));
    REQUIRE_FALSE(getter->value(doc, tape.get()));

    getter = get::simple_value_t::create(key("countArray/10"));
    REQUIRE_FALSE(getter->value(doc, tape.get()));

    getter = get::simple_value_t::create(key("countDict/invalid"));
    REQUIRE_FALSE(getter->value(doc, tape.get()));
}
