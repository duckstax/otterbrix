#include <catch2/catch.hpp>
#include <document/document_id.hpp>

using components::document::document_id_t;

TEST_CASE("document_id") {
    REQUIRE(document_id_t().to_string().size() == 24);
    REQUIRE(document_id_t(2000).get_timestamp() == 2000);
    REQUIRE(document_id_t().get_timestamp() == static_cast<oid::timestamp_value_t>(time(nullptr)));
}
