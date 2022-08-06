#include <catch2/catch.hpp>
#include <components/document/document_view.hpp>
#include <components/tests/generaty.hpp>

using components::document::document_t;
using components::document::document_view_t;
using ::document::impl::value_type;

TEST_CASE("document_view::get_value") {
    auto doc = gen_doc(1);
    document_view_t view(doc);

    REQUIRE(view.get_value() != nullptr);
    REQUIRE(view.get_value()->type() == value_type::dict);

    REQUIRE(view.get_value("count") != nullptr);
    REQUIRE(view.get_value("count")->type() == value_type::number);
    REQUIRE(view.get_value("count")->as_unsigned() == 1);

    REQUIRE(view.get_value("countStr") != nullptr);
    REQUIRE(view.get_value("countStr")->type() == value_type::string);
    REQUIRE(view.get_value("countStr")->as_string().as_string() == "1");

    REQUIRE(view.get_value("countArray") != nullptr);
    REQUIRE(view.get_value("countArray")->type() == value_type::array);

    REQUIRE(view.get_value("countDict") != nullptr);
    REQUIRE(view.get_value("countDict")->type() == value_type::dict);

    REQUIRE(view.get_value("countArray.1") != nullptr);
    REQUIRE(view.get_value("countArray.1")->is_unsigned());
    REQUIRE(view.get_value("countArray.1")->as_unsigned() == 2);

    REQUIRE(view.get_value("countDict.even") != nullptr);
    REQUIRE(view.get_value("countDict.even")->type() == value_type::boolean);
    REQUIRE(view.get_value("countDict.even")->as_bool() == false);

    REQUIRE(view.get_value("other") == nullptr);
    REQUIRE(view.get_value("countArray.10") == nullptr);
    REQUIRE(view.get_value("countDict.other") == nullptr);
}
