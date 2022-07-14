#include <catch2/catch.hpp>

#include "../parser.hpp"
#include "components/document/document.hpp"

TEST_CASE("main ql") {
    auto d = components::document::document_from_json("{\"$and\": [{\"count\": {\"$gt\": 10}}, {\"count\": {\"$lte\": 50}}]}");
    auto find = parse_find_condition(d);
}