#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"
#include <catch2/catch.hpp>
#include <components/sql_new/parser/parser.h>
#include <ql/ql_param_statement.hpp>

using namespace components::sql_new;

#define TEST_JOIN(QUERY, RESULT, PARAMS)                                                                               \
    SECTION(QUERY) {                                                                                                   \
        auto resource = std::pmr::synchronized_pool_resource();                                                        \
        transform::transformer transformer(&resource);                                                                 \
        components::ql::ql_param_statement_t agg(&resource);                                                           \
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

TEST_CASE("sql_new::join") {
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

        TEST_JOIN(R"_(select * from col1 cross join col2;)_",
                  R"_($join: {$type: cross, $aggregate: {}, $aggregate: {}})_",
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
