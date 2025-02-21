#include "components/sql_new/transformer/expressions/parsed_expression.hpp"
#include "sql_new/parser/pg_functions.h"
#include "sql_new/transformer/tokens.hpp"
#include "sql_new/transformer/transformer.hpp"
#include <catch2/catch.hpp>
#include <components/sql_new/parser/parser.h>
#include <iostream>

TEST_CASE("parser::base") {
    auto test = raw_parser("select * from tbl1 join tbl2 on tbl1.id = tbl2.id_tbl1;");
    PGListCell a = test->lst.front();
    if (nodeTag(a.data) == T_SelectStmt) {
        auto s = (SelectStmt*) a.data;
        auto t = (ResTarget*) (s->targetList->lst.front().data);
        auto j = (JoinExpr*) (s->fromClause->lst.front().data);
        auto b = 0;
    }

    auto b = (InsertStmt*) (raw_parser("insert into test (a, b, c) values (1, 2, 3);")->lst.front().data);
    raw_parser("create table test(a integer, b varchar(200));");
    raw_parser("drop table test;");
    raw_parser("update test set a = 1, b = 2 where test.a == 0;");
}
