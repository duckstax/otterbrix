#include <catch2/catch.hpp>
#include <components/tests/generaty.hpp>
#include <components/document/mutable/mutable_value.hpp>
#include <services/disk/index_disk.hpp>

using services::disk::index_disk;
using components::document::document_id_t;
using document::wrapper_value_t;

std::string gen_str_value(int i, std::size_t size = 5) {
    auto s = std::to_string(i);
    while (s.size() < size) {
        s.insert(0, "0");
    }
    return s;
}

TEST_CASE("index_disk::string") {
    std::filesystem::path path{"/tmp/index_disk/string"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = index_disk(path, index_disk::compare::str);
    std::vector<document::retained_const_t<document::impl::value_t>> trash(100, nullptr);

    for (int i = 1; i <= 100; ++i) {
        trash.push_back(document::impl::new_value(gen_str_value(i)));
        index.insert(wrapper_value_t{trash.back()}, document_id_t{gen_id(i)});
    }

    trash.push_back(document::impl::new_value(gen_str_value(1)));
    auto res = index.find(wrapper_value_t{trash.back()});
    REQUIRE(res.size() == 1);

    trash.push_back(document::impl::new_value(gen_str_value(10)));
    res = index.find(wrapper_value_t{trash.back()});
    REQUIRE(res.size() == 1);

    trash.push_back(document::impl::new_value(gen_str_value(100)));
    res = index.find(wrapper_value_t{trash.back()});
    REQUIRE(res.size() == 1);

    trash.push_back(document::impl::new_value(gen_str_value(101)));
    res = index.find(wrapper_value_t{trash.back()});
    REQUIRE(res.empty());

    trash.push_back(document::impl::new_value(gen_str_value(0)));
    res = index.find(wrapper_value_t{trash.back()});
    REQUIRE(res.empty());

    trash.push_back(document::impl::new_value(gen_str_value(10)));
    res = index.lower_bound(wrapper_value_t{trash.back()});
    REQUIRE(res.size() == 9);

    trash.push_back(document::impl::new_value(gen_str_value(90)));
    res = index.upper_bound(wrapper_value_t{trash.back()});
    REQUIRE(res.size() == 10);

    for (int i = 2; i <= 100; i += 2) {
        trash.push_back(document::impl::new_value(gen_str_value(i)));
        index.remove(wrapper_value_t{trash.back()});
    }

    trash.push_back(document::impl::new_value(gen_str_value(2)));
    res = index.find(wrapper_value_t{trash.back()});
    REQUIRE(res.empty());

    trash.push_back(document::impl::new_value(gen_str_value(10)));
    res = index.lower_bound(wrapper_value_t{trash.back()});
    REQUIRE(res.size() == 5);

    trash.push_back(document::impl::new_value(gen_str_value(90)));
    res = index.upper_bound(wrapper_value_t{trash.back()});
    REQUIRE(res.size() == 5);
}
