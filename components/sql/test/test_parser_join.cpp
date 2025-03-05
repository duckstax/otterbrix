#include <catch2/catch.hpp>
#include <components/sql/parser.hpp>

using namespace components;

#define TEST_JOIN(QUERY, RESULT, PARAMS)                                                                               \
    SECTION(QUERY) {                                                                                                   \
        auto res = sql::parse(&resource, QUERY);                                                                       \
        auto ql = res.ql;                                                                                              \
        REQUIRE(std::holds_alternative<ql::join_t>(ql));                                                               \
        auto& join = std::get<ql::join_t>(ql);                                                                         \
        std::stringstream s;                                                                                           \
        s << join;                                                                                                     \
        REQUIRE(s.str() == RESULT);                                                                                    \
        REQUIRE(join.parameters().parameters.size() == PARAMS.size());                                                 \
        for (auto i = 0ul; i < PARAMS.size(); ++i) {                                                                   \
            REQUIRE(join.parameter(core::parameter_id_t(uint16_t(i))) == PARAMS.at(i));                                \
        }                                                                                                              \
    }

using v = document::value_t;
using vec = std::vector<v>;

TEST_CASE("parser::join") {
    auto resource = std::pmr::synchronized_pool_resource();

    INFO("join types") {
        TEST_JOIN(R"_(select * from col1 join col2 on col1.id = col2.id_col1;)_",
                  R"_($join: {$type: inner, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}})_",
                  vec());

        TEST_JOIN(R"_(select * from col1 inner join col2 on col1.id = col2.id_col1;)_",
                  R"_($join: {$type: inner, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}})_",
                  vec());

        TEST_JOIN(R"_(select * from col1 full outer join col2 on col1.id = col2.id_col1;)_",
                  R"_($join: {$type: full, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}})_",
                  vec());

        TEST_JOIN(R"_(select * from col1 left outer join col2 on col1.id = col2.id_col1;)_",
                  R"_($join: {$type: left, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}})_",
                  vec());

        TEST_JOIN(R"_(select * from col1 right outer join col2 on col1.id = col2.id_col1;)_",
                  R"_($join: {$type: right, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}})_",
                  vec());

        TEST_JOIN(R"_(select * from col1 cross join col2 on col1.id = col2.id_col1;)_",
                  R"_($join: {$type: cross, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}})_",
                  vec());
    }

    INFO("join specifics") {
        TEST_JOIN(
            R"_(select * from col1 join col2 on col1.id = col2.id_col1 and col1.name = col2.name;)_",
            R"_($join: {$type: inner, $aggregate: {}, $aggregate: {}, $and: ["id": {$eq: "id_col1"}, "name": {$eq: "name"}]})_",
            vec());

        TEST_JOIN(
            R"_(select * from col1 join col2 on col1.id = col2.id_col1 )_"
            R"_(join col3 on col1.id = col3.id_col1 and col2.id = col3.id_col2;)_",
            R"_($join: {$type: inner, $join: {$type: inner, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}}, )_"
            R"_($aggregate: {}, $and: ["id": {$eq: "id_col1"}, "id": {$eq: "id_col2"}]})_",
            vec());
    }
}
