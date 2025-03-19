#include <catch2/catch.hpp>
#include <components/sql/parser/parser.h>

TEST_CASE("sql::raw_parser_create") {
    auto test = raw_parser(R"_(create table test(a integer, b varchar(200));)_");
    REQUIRE(nodeTag(linitial(test)) == T_CreateStmt);

    test = raw_parser(R"_(create table users (id serial primary key, name varchar(100),)_"
                      R"_(email varchar(100) unique, created_at timestamp default current_timestamp);)_");
    REQUIRE(nodeTag(linitial(test)) == T_CreateStmt);

    test = raw_parser(R"_(create table orders (order_id serial primary key, user_id int references users(id),)_"
                      R"_(total_amount decimal not null);)_");
    REQUIRE(nodeTag(linitial(test)) == T_CreateStmt);

    test = raw_parser(R"_(create table products (product_id serial primary key, name varchar(200) not null,)_"
                      R"_(price decimal check (price >= 0));)_");
    REQUIRE(nodeTag(linitial(test)) == T_CreateStmt);

    test = raw_parser(R"_(create table employees (employee_id serial primary key, name varchar(100) unique,)_"
                      R"_(department varchar(100), salary decimal);)_");
    REQUIRE(nodeTag(linitial(test)) == T_CreateStmt);

    test = raw_parser(R"_(create table transactions (transaction_id serial primary key, account_id int,)_"
                      R"_(amount decimal not null, transaction_date timestamp not null, status varchar(50));)_");
    REQUIRE(nodeTag(linitial(test)) == T_CreateStmt);
}

TEST_CASE("sql::raw_parser_select") {
    auto test = raw_parser(R"_(select * from tbl1 )_"
                           R"_(join tbl2 on tbl1.id = tbl2.id_tbl1;)_");
    REQUIRE(nodeTag(linitial(test)) == T_SelectStmt);

    test = raw_parser(R"_(select col1, col2, count(*) from table1 t1 )_"
                      R"_(join table2 t2 on t1.id = t2.id group by col1, col2 )_"
                      R"_(having count(*) > 10 order by col1 desc limit 100 offset 50;)_");
    REQUIRE(nodeTag(linitial(test)) == T_SelectStmt);

    test = raw_parser(R"_(select name, (select max(salary) from employees e where e.department_id = d.id)_"
                      R"_() as max_salary from departments d;)_");
    REQUIRE(nodeTag(linitial(test)) == T_SelectStmt);
}

TEST_CASE("sql::raw_parser_update") {
    auto test = raw_parser(R"_(update test set a = 1, b = 2 where test.a == 0;)_");
    REQUIRE(nodeTag(linitial(test)) == T_UpdateStmt);

    test = raw_parser(R"_(update employees set salary = salary * 1.1)_"
                      R"_(where department_id in (select id from departments where name = 'Sales');)_");
    REQUIRE(nodeTag(linitial(test)) == T_UpdateStmt);
}

TEST_CASE("sql::raw_parser_insert") {
    auto test = raw_parser(R"_(insert into employees (id, name, department_id) )_"
                           R"_(select id, name, department_id from old_employees where status = 'active';)_");
    REQUIRE(nodeTag(linitial(test)) == T_InsertStmt);
}

TEST_CASE("sql::raw_parser_drop") {
    auto test = raw_parser("drop table test;");
    REQUIRE(nodeTag(linitial(test)) == T_DropStmt);

    test = raw_parser(R"_(delete from employees )_"
                      R"_(where department_id not in (select id from departments where name like 'Sales%');)_");
    REQUIRE(nodeTag(linitial(test)) == T_DeleteStmt);
}

TEST_CASE("sql::raw_parser_index") {
    auto test = raw_parser(R"_(create index idx_name on employees (salary, department_id);)_");
    REQUIRE(nodeTag(linitial(test)) == T_IndexStmt);

    test = raw_parser("drop index if exists idx_name;");
    REQUIRE(nodeTag(linitial(test)) == T_DropStmt);
}
