#include <catch2/catch.hpp>
#include <components/cursor/cursor.hpp>
#include <components/tests/generaty.hpp>
#include <core/pmr.hpp>

using namespace core::pmr;

TEST_CASE("cursor::construction") {
    auto resource = std::pmr::synchronized_pool_resource();
    INFO("empty cursor") {
        auto cursor = components::cursor::make_cursor(&resource);
        REQUIRE(cursor->is_success());
        REQUIRE_FALSE(cursor->is_error());
    }
    INFO("failed operation cursor") {
        auto cursor = components::cursor::make_cursor(&resource, components::cursor::operation_status_t::failure);
        REQUIRE_FALSE(cursor->is_success());
        REQUIRE_FALSE(cursor->is_error());
    }
    INFO("successful operation cursor") {
        auto cursor = components::cursor::make_cursor(&resource, components::cursor::operation_status_t::success);
        REQUIRE(cursor->is_success());
        REQUIRE_FALSE(cursor->is_error());
    }
    INFO("error cursor") {
        std::string description = "error description";
        auto cursor =
            components::cursor::make_cursor(&resource, components::cursor::error_code_t::other_error, description);
        REQUIRE_FALSE(cursor->is_success());
        REQUIRE(cursor->is_error());
        REQUIRE(cursor->get_error().type == components::cursor::error_code_t::other_error);
        REQUIRE(cursor->get_error().what == description);
    }
}

TEST_CASE("cursor::sort") {
    auto resource = std::pmr::synchronized_pool_resource();
    components::cursor::cursor_t cursor(&resource);
    for (int i = 0; i < 10; ++i) {
        auto* sub_cursor = new components::cursor::sub_cursor_t(&resource, {});
        for (int j = 0; j < 10; ++j) {
            sub_cursor->append(gen_doc(10 * i + j + 1, &resource));
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
