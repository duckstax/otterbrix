#include <catch2/catch.hpp>
#include <components/cursor/cursor.hpp>
#include <components/tests/generaty.hpp>
#include <core/pmr.hpp>

using namespace core::pmr;

TEST_CASE("cursor::construction") {
    INFO("empty cursor") {
        auto cursor = components::cursor::make_cursor(default_resource());
        REQUIRE(cursor->is_success());
        REQUIRE_FALSE(cursor->is_error());
    }
    INFO("failed operation cursor") {
        auto cursor =
            components::cursor::make_cursor(default_resource(), components::cursor::operation_status_t::failure);
        REQUIRE_FALSE(cursor->is_success());
        REQUIRE_FALSE(cursor->is_error());
    }
    INFO("successful operation cursor") {
        auto cursor =
            components::cursor::make_cursor(default_resource(), components::cursor::operation_status_t::success);
        REQUIRE(cursor->is_success());
        REQUIRE_FALSE(cursor->is_error());
    }
    INFO("error cursor") {
        std::string description = "error description";
        auto cursor = components::cursor::make_cursor(default_resource(),
                                                      components::cursor::error_code_t::other_error,
                                                      description);
        REQUIRE_FALSE(cursor->is_success());
        REQUIRE(cursor->is_error());
        REQUIRE(cursor->get_error().type == components::cursor::error_code_t::other_error);
        REQUIRE(cursor->get_error().what == description);
    }
}

TEST_CASE("cursor::sort") {
    components::cursor::cursor_t cursor(default_resource());
    for (int i = 0; i < 10; ++i) {
        auto* sub_cursor =
            new components::cursor::sub_cursor_t(default_resource(), actor_zeta::address_t::empty_address());
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
