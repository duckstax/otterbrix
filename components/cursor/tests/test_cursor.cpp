#include <catch2/catch.hpp>
#include <components/tests/generaty.hpp>
#include <components/cursor/cursor.hpp>

TEST_CASE("cursor::sort") {
    components::cursor::cursor_t cursor(std::pmr::get_default_resource());
    for (int i = 0; i < 10; ++i) {
        auto *sub_cursor = new components::cursor::sub_cursor_t(std::pmr::get_default_resource(), actor_zeta::address_t::empty_address());
        for (int j = 0; j < 10; ++j) {
            sub_cursor->append(document_view_t(gen_doc(10 * i + j + 1)));
        }
        cursor.push(sub_cursor);
    }
    REQUIRE(cursor.size() == 100);
    cursor.sort([](components::cursor::data_ptr doc1, components::cursor::data_ptr doc2) {
        return doc1->get_long("count") > doc2->get_long("count");
    });
    REQUIRE(cursor.get(0)->get_long("count") == 100);
    REQUIRE(cursor.get(99)->get_long("count") == 1);
    cursor.sort([](components::cursor::data_ptr doc1, components::cursor::data_ptr doc2) {
        return doc1->get_long("count") < doc2->get_long("count");
    });
    REQUIRE(cursor.get(0)->get_long("count") == 1);
    REQUIRE(cursor.get(99)->get_long("count") == 100);
}
