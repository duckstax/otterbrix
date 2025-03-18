#include <catch2/catch.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;

#define TEST_JOIN(QUERY, RESULT, PARAMS)                                                                               \
    SECTION(QUERY) {                                                                                                   \
        auto resource = std::pmr::synchronized_pool_resource();                                                        \
        transform::transformer transformer(&resource);                                                                 \
        components::logical_plan::parameter_node_t agg(&resource);                                                     \
        auto select = raw_parser(QUERY)->lst.front().data;                                                             \
        auto node = transformer.transform(transform::pg_cell_to_node_cast(select), &agg);                              \
        REQUIRE(node->to_string() == RESULT);                                                                          \
        REQUIRE(agg.parameters().parameters.size() == PARAMS.size());                                                  \
        for (auto i = 0ul; i < PARAMS.size(); ++i) {                                                                   \
            REQUIRE(agg.parameter(core::parameter_id_t(uint16_t(i))) == PARAMS.at(i));                                 \
        }                                                                                                              \
    }

using v = components::document::value_t;
using vec = std::vector<v>;

TEST_CASE("sql::join") {
    auto resource = std::pmr::synchronized_pool_resource();

    INFO("join types") {
        TEST_JOIN(R"_(select * from col1 join col2 on col1.id = col2.id_col1;)_",
                  R"_($aggregate: {$join: {$type: inner, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}}})_",
                  vec());

        TEST_JOIN(R"_(select * from col1 inner join col2 on col1.id = col2.id_col1;)_",
                  R"_($aggregate: {$join: {$type: inner, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}}})_",
                  vec());

        TEST_JOIN(R"_(select * from col1 full outer join col2 on col1.id = col2.id_col1;)_",
                  R"_($aggregate: {$join: {$type: full, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}}})_",
                  vec());

        TEST_JOIN(R"_(select * from col1 left outer join col2 on col1.id = col2.id_col1;)_",
                  R"_($aggregate: {$join: {$type: left, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}}})_",
                  vec());

        TEST_JOIN(R"_(select * from col1 right outer join col2 on col1.id = col2.id_col1;)_",
                  R"_($aggregate: {$join: {$type: right, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}}})_",
                  vec());

        TEST_JOIN(R"_(select * from col1 cross join col2;)_",
                  R"_($aggregate: {$join: {$type: cross, $aggregate: {}, $aggregate: {}, $all_true}})_",
                  vec());
    }

    INFO("join specifics") {
        TEST_JOIN(
            R"_(select col1.id, col2.id_col1 from db.col as col1 JOIN col2 on col1.id = col2.id_col1;)_",
            R"_($aggregate: {$join: {$type: inner, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}}, $group: {id, id_col1}})_",
            vec());

        TEST_JOIN(
            R"_(select * from col1 join col2 on col1.id = col2.id_col1 and col1.name = col2.name;)_",
            R"_($aggregate: {$join: {$type: inner, $aggregate: {}, $aggregate: {}, $and: ["id": {$eq: "id_col1"}, "name": {$eq: "name"}]}})_",
            vec());

        TEST_JOIN(
            R"_(select * from col1 join col2 on col1.id = col2.id_col1 )_"
            R"_(join col3 on col1.id = col3.id_col1 and col2.id = col3.id_col2;)_",
            R"_($aggregate: {$join: {$type: inner, $join: {$type: inner, $aggregate: {}, $aggregate: {}, "id": {$eq: "id_col1"}}, )_"
            R"_($aggregate: {}, $and: ["id": {$eq: "id_col1"}, "id": {$eq: "id_col2"}]}})_",
            vec());
    }
}
