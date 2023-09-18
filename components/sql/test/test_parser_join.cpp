#include <catch2/catch.hpp>
#include <components/sql/parser.hpp>

using namespace components;

#define TEST_SIMPLE_JOIN(QUERY, RESULT, PARAMS)                          \
    SECTION(QUERY) {                                                     \
        auto res = sql::parse(resource, QUERY);                          \
        auto ql = res.ql;                                                \
        REQUIRE(std::holds_alternative<ql::join_t>(ql));                 \
        auto& join = std::get<ql::join_t>(ql);                           \
        std::stringstream s;                                             \
        s << join;                                                       \
        REQUIRE(s.str() == RESULT);                                      \
        REQUIRE(join.parameters().size() == PARAMS.size());              \
        for (auto i = 0ul; i < PARAMS.size(); ++i) {                     \
            REQUIRE(join.parameter(core::parameter_id_t(uint16_t(i)))    \
                    == PARAMS.at(i));                                    \
        }                                                                \
    }

using v = ::document::wrapper_value_t;
using vec = std::vector<v>;

TEST_CASE("parser::join") {

    auto* resource = std::pmr::get_default_resource();

    TEST_SIMPLE_JOIN(R"_(select * from col1 join col2 on;)_",
                     R"_($join: {$type: inner, $aggregate: {}, $aggregate: {}})_",
                     vec());

}
