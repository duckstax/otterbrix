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
    REQUIRE(view.get_value("countArray.1")->type() == value_type::number);
    REQUIRE(view.get_value("countArray.1")->as_unsigned() == 2);

    REQUIRE(view.get_value("countDict.even") != nullptr);
    REQUIRE(view.get_value("countDict.even")->type() == value_type::boolean);
    REQUIRE(view.get_value("countDict.even")->as_bool() == false);

    REQUIRE(view.get_value("other") == nullptr);
    REQUIRE(view.get_value("countArray.10") == nullptr);
    REQUIRE(view.get_value("countDict.other") == nullptr);
}

TEST_CASE("document_view::set") {
    auto doc = gen_doc(1);

    std::string key("newValue");
    std::string value("new value");
    doc->set(key, value);

    REQUIRE(document_view_t(doc).get_value(key) != nullptr);
    REQUIRE(document_view_t(doc).get_value(key)->type() == value_type::string);
    REQUIRE(document_view_t(doc).get_value(key)->as_string().as_string() == value);

    REQUIRE(document_view_t(doc).get_value()->as_dict()->get(key) != nullptr);
    REQUIRE(document_view_t(doc).get_value()->as_dict()->get(key)->type() == value_type::string);
    REQUIRE(document_view_t(doc).get_value()->as_dict()->get(key)->as_string().as_string() == value);

    value = "super new value";
    doc->set(key, value);

    REQUIRE(document_view_t(doc).get_value(key) != nullptr);
    REQUIRE(document_view_t(doc).get_value(key)->type() == value_type::string);
    REQUIRE(document_view_t(doc).get_value(key)->as_string().as_string() == value);

    REQUIRE(document_view_t(doc).get_value()->as_dict()->get(key) != nullptr);
    REQUIRE(document_view_t(doc).get_value()->as_dict()->get(key)->type() == value_type::string);
    REQUIRE(document_view_t(doc).get_value()->as_dict()->get(key)->as_string().as_string() == value);
}

TEST_CASE("document_view::update") {
    auto doc = gen_doc(1);

    REQUIRE(document_view_t(doc).get_value("count") != nullptr);
    REQUIRE(document_view_t(doc).get_value("count")->type() == value_type::number);
    REQUIRE(document_view_t(doc).get_value("count")->as_int() == 1);
    REQUIRE(document_view_t(doc).get_value()->as_dict()->get("count") != nullptr);
    REQUIRE(document_view_t(doc).get_value()->as_dict()->get("count")->as_int() == 1);

    doc->update(*document_from_json(R"({"$set": {"count": 100}})").get());
    REQUIRE(document_view_t(doc).get_value("count") != nullptr);
    REQUIRE(document_view_t(doc).get_value("count")->type() == value_type::number);
    REQUIRE(document_view_t(doc).get_value("count")->as_int() == 100);
    REQUIRE(document_view_t(doc).get_value()->as_dict()->get("count") != nullptr);
    REQUIRE(document_view_t(doc).get_value()->as_dict()->get("count")->as_int() == 100);

    doc->update(*document_from_json(R"({"$inc": {"count": 1}})").get());
    REQUIRE(document_view_t(doc).get_value("count") != nullptr);
    REQUIRE(document_view_t(doc).get_value("count")->type() == value_type::number);
    REQUIRE(document_view_t(doc).get_value("count")->as_int() == 101);
    REQUIRE(document_view_t(doc).get_value()->as_dict()->get("count") != nullptr);
    REQUIRE(document_view_t(doc).get_value()->as_dict()->get("count")->as_int() == 101);

    doc->update(*document_from_json(R"({"$inc": {"count": 10}})").get());
    REQUIRE(document_view_t(doc).get_value("count") != nullptr);
    REQUIRE(document_view_t(doc).get_value("count")->type() == value_type::number);
    REQUIRE(document_view_t(doc).get_value("count")->as_int() == 111);
    REQUIRE(document_view_t(doc).get_value()->as_dict()->get("count") != nullptr);
    REQUIRE(document_view_t(doc).get_value()->as_dict()->get("count")->as_int() == 111);

    doc->update(*document_from_json(R"({"$inc": {"count": -1}})").get());
    REQUIRE(document_view_t(doc).get_value("count") != nullptr);
    REQUIRE(document_view_t(doc).get_value("count")->type() == value_type::number);
    REQUIRE(document_view_t(doc).get_value("count")->as_int() == 110);
    REQUIRE(document_view_t(doc).get_value()->as_dict()->get("count") != nullptr);
    REQUIRE(document_view_t(doc).get_value()->as_dict()->get("count")->as_int() == 110);

    REQUIRE(document_view_t(doc).get_value("count2") == nullptr);
    doc->update(*document_from_json(R"({"$set": {"count2": 100}})").get());
    REQUIRE(document_view_t(doc).get_value("count2") != nullptr);
    REQUIRE(document_view_t(doc).get_value("count2")->type() == value_type::number);
    REQUIRE(document_view_t(doc).get_value("count2")->as_int() == 100);
    REQUIRE(document_view_t(doc).get_value()->as_dict()->get("count2") != nullptr);
    REQUIRE(document_view_t(doc).get_value()->as_dict()->get("count2")->as_int() == 100);
}

TEST_CASE("document_view::value from json") {
    auto json = R"(
{
  "_id": "000000000000000000000001",
  "count": 1,
  "countBool": true,
  "countDouble": 1.1,
  "countStr": "1",
  "countArray": [1, 2, 3, 4, 5],
  "countDict": {
    "even": false,
    "five": false,
    "odd": true,
    "three": false
  }
}
    )";
    auto doc = document_from_json(json);
    document_view_t view(doc);

    REQUIRE(view.get_value() != nullptr);
    REQUIRE(view.get_value("count") != nullptr);
    REQUIRE(view.get_value("count")->type() == value_type::number);
    REQUIRE(view.get_value("count")->as_int() == 1);

    REQUIRE(view.get_value()->as_dict()->get("count") != nullptr);
    REQUIRE(view.get_value()->as_dict()->get("count")->type() == value_type::number);
    REQUIRE(view.get_value()->as_dict()->get("count")->as_int() == 1);
}
