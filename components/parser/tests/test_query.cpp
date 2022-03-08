#include <catch2/catch.hpp>
#include <components/document/mutable/mutable_array.h>
#include <components/document/mutable/mutable_dict.h>
#include <components/parser/find_condition.hpp>

using namespace components::parser;

components::document::document_ptr gen_doc() {
    auto doc = document::impl::mutable_dict_t::new_dict().detach();
    doc->set("_id", std::string("1"));
    doc->set("name", std::string("Rex"));
    doc->set("type", std::string("dog"));
    doc->set("age", 6);
    doc->set("male", true);
    auto ar = ::document::impl::mutable_array_t::new_array().detach();
    ar->append(3);
    ar->append(4);
    ar->append(5);
    doc->set("ar", ar);
    return components::document::make_document(doc);
}

TEST_CASE("query_t create") {
    auto doc = gen_doc();

    REQUIRE(find_condition_eq<bool>("male", true).is_fit(doc));
    REQUIRE(find_condition_ne<bool>("male", false).is_fit(doc));

    REQUIRE(find_condition_eq<long>("age", 6).is_fit(doc));
    REQUIRE(find_condition_gt<long>("age", 4).is_fit(doc));
    REQUIRE(find_condition_lt<long>("age", 7).is_fit(doc));
    REQUIRE(find_condition_gte<long>("age", 4).is_fit(doc));
    REQUIRE(find_condition_lte<long>("age", 6).is_fit(doc));
    REQUIRE_FALSE(find_condition_ne<long>("age", 6).is_fit(doc));
    REQUIRE_FALSE(find_condition_gt<long>("age", 7).is_fit(doc));
    REQUIRE_FALSE(find_condition_lt<long>("age", 6).is_fit(doc));
    REQUIRE_FALSE(find_condition_gte<long>("age", 7).is_fit(doc));
    REQUIRE_FALSE(find_condition_lte<long>("age", 5).is_fit(doc));

    REQUIRE(find_condition_eq<std::string>("name", "Rex").is_fit(doc));
    REQUIRE(find_condition_ne<std::string>("type", "cat").is_fit(doc));
}


TEST_CASE("query_t between/any/all") {
    auto doc = gen_doc();
    std::vector<long> v1 = {3, 4, 5, 6, 7};
    std::vector<long> v2 = {1, 2, 3};
    std::vector<long> v3 = {1, 2};

    REQUIRE(find_condition_between<long>("age", 4, 6).is_fit(doc));
    REQUIRE_FALSE(find_condition_between<long>("age", 1, 5).is_fit(doc));
    REQUIRE_FALSE(find_condition_between<long>("age", 7, 12).is_fit(doc));

    REQUIRE(find_condition_any<long>("ar", v1).is_fit(doc));
    REQUIRE(find_condition_any<long>("ar", v2).is_fit(doc));
    REQUIRE_FALSE(find_condition_any<long>("ar", v3).is_fit(doc));

    REQUIRE(find_condition_all<long>("ar", v1).is_fit(doc));
    REQUIRE_FALSE(find_condition_all<long>("ar", v2).is_fit(doc));
    REQUIRE_FALSE(find_condition_all<long>("ar", v3).is_fit(doc));
}


TEST_CASE("query_t regex") {
    auto doc = gen_doc();

    REQUIRE_FALSE(find_condition_regex("name", "^Re$").is_fit(doc));
    REQUIRE(find_condition_regex("name", "^Re").is_fit(doc));
    REQUIRE_FALSE(find_condition_regex("type", "^og$").is_fit(doc));
    REQUIRE(find_condition_regex("type", "og$").is_fit(doc));
    REQUIRE(find_condition_regex("type", "dog").is_fit(doc));
    REQUIRE_FALSE(find_condition_regex("type", "Dog").is_fit(doc));
}


TEST_CASE("query_t and/or/not") {
    auto doc = gen_doc();

    REQUIRE(find_condition_and({make_condition<find_condition_eq<std::string>>("name", "Rex"),
                                make_condition<find_condition_eq<long>>("age", 6)})
            .is_fit(doc));
    REQUIRE_FALSE(find_condition_and({make_condition<find_condition_eq<std::string>>("name", "Rex"),
                                      make_condition<find_condition_eq<long>>("age", 5)})
                  .is_fit(doc));
    REQUIRE_FALSE(find_condition_and({make_condition<find_condition_eq<std::string>>("name", "Re"),
                                      make_condition<find_condition_eq<long>>("age", 6)})
                  .is_fit(doc));
    REQUIRE_FALSE(find_condition_and({make_condition<find_condition_eq<std::string>>("name", "Re"),
                                      make_condition<find_condition_eq<long>>("age", 5)})
                  .is_fit(doc));

    REQUIRE(find_condition_or({make_condition<find_condition_eq<std::string>>("name", "Rex"),
                               make_condition<find_condition_eq<long>>("age", 6)})
            .is_fit(doc));
    REQUIRE(find_condition_or({make_condition<find_condition_eq<std::string>>("name", "Rex"),
                               make_condition<find_condition_eq<long>>("age", 5)})
            .is_fit(doc));
    REQUIRE(find_condition_or({make_condition<find_condition_eq<std::string>>("name", "Re"),
                               make_condition<find_condition_eq<long>>("age", 6)})
            .is_fit(doc));
    REQUIRE_FALSE(find_condition_or({make_condition<find_condition_eq<std::string>>("name", "Re"),
                                     make_condition<find_condition_eq<long>>("age", 5)})
                  .is_fit(doc));

    REQUIRE_FALSE(find_condition_not({make_condition<find_condition_eq<std::string>>("name", "Rex")})
                  .is_fit(doc));
    REQUIRE(find_condition_not({make_condition<find_condition_eq<std::string>>("name", "Re")})
            .is_fit(doc));
}
