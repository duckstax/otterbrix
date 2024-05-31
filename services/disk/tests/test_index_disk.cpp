#include <catch2/catch.hpp>
#include <components/tests/generaty.hpp>
#include <services/disk/index_disk.hpp>

using components::document::document_id_t;
using document::wrapper_value_t;
using services::disk::index_disk_t;

std::string gen_str_value(int i, std::size_t size = 5) {
    auto s = std::to_string(i);
    while (s.size() < size) {
        s.insert(0, "0");
    }
    return s;
}

document::wrapper_value_t value(int64_t n) {
    static document::retained_const_t<document::impl::value_t> value;
    value = document::impl::new_value(n);
    return wrapper_value_t{value.get()};
}

document::wrapper_value_t value(uint64_t n) {
    static document::retained_const_t<document::impl::value_t> value;
    value = document::impl::new_value(n);
    return wrapper_value_t{value.get()};
}

document::wrapper_value_t value(double n) {
    static document::retained_const_t<document::impl::value_t> value;
    value = document::impl::new_value(n);
    return wrapper_value_t{value.get()};
}

document::wrapper_value_t value_str(int n) {
    static document::retained_const_t<document::impl::value_t> value;
    value = document::impl::new_value(gen_str_value(n));
    return wrapper_value_t{value.get()};
}

TEST_CASE("index_disk::string") {
    std::filesystem::path path{"/tmp/index_disk/string"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = index_disk_t(path, core::type::str);

    for (int i = 1; i <= 100; ++i) {
        index.insert(value_str(i), document_id_t{gen_id(i)});
    }

    REQUIRE(index.find(value_str(1)).size() == 1);
    REQUIRE(index.find(value_str(1)).front() == document_id_t{gen_id(1)});
    REQUIRE(index.find(value_str(10)).size() == 1);
    REQUIRE(index.find(value_str(10)).front() == document_id_t{gen_id(10)});
    REQUIRE(index.find(value_str(100)).size() == 1);
    REQUIRE(index.find(value_str(100)).front() == document_id_t{gen_id(100)});
    REQUIRE(index.find(value_str(101)).empty());
    REQUIRE(index.find(value_str(0)).empty());

    REQUIRE(index.lower_bound(value_str(10)).size() == 9);
    REQUIRE(index.upper_bound(value_str(90)).size() == 10);

    for (int i = 2; i <= 100; i += 2) {
        index.remove(value_str(i));
    }

    REQUIRE(index.find(value_str(2)).empty());
    REQUIRE(index.lower_bound(value_str(10)).size() == 5);
    REQUIRE(index.upper_bound(value_str(90)).size() == 5);
}

TEST_CASE("index_disk::int32") {
    std::filesystem::path path{"/tmp/index_disk/int32"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = index_disk_t(path, core::type::int32);

    for (int i = 1; i <= 100; ++i) {
        index.insert(value(int64_t(i)), document_id_t{gen_id(i)});
    }

    REQUIRE(index.find(value(1l)).size() == 1);
    REQUIRE(index.find(value(1l)).front() == document_id_t{gen_id(1)});
    REQUIRE(index.find(value(10l)).size() == 1);
    REQUIRE(index.find(value(10l)).front() == document_id_t{gen_id(10)});
    REQUIRE(index.find(value(100l)).size() == 1);
    REQUIRE(index.find(value(100l)).front() == document_id_t{gen_id(100)});
    REQUIRE(index.find(value(101l)).empty());
    REQUIRE(index.find(value(0l)).empty());

    REQUIRE(index.lower_bound(value(10l)).size() == 9);
    REQUIRE(index.upper_bound(value(90l)).size() == 10);

    for (int i = 2; i <= 100; i += 2) {
        index.remove(value(int64_t(i)));
    }

    REQUIRE(index.find(value(2l)).empty());
    REQUIRE(index.lower_bound(value(10l)).size() == 5);
    REQUIRE(index.upper_bound(value(90l)).size() == 5);
}

TEST_CASE("index_disk::uint32") {
    std::filesystem::path path{"/tmp/index_disk/uint32"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = index_disk_t(path, core::type::uint32);

    for (int i = 1; i <= 100; ++i) {
        index.insert(value(uint64_t(i)), document_id_t{gen_id(i)});
    }

    REQUIRE(index.find(value(1ul)).size() == 1);
    REQUIRE(index.find(value(1ul)).front() == document_id_t{gen_id(1)});
    REQUIRE(index.find(value(10ul)).size() == 1);
    REQUIRE(index.find(value(10ul)).front() == document_id_t{gen_id(10)});
    REQUIRE(index.find(value(100ul)).size() == 1);
    REQUIRE(index.find(value(100ul)).front() == document_id_t{gen_id(100)});
    REQUIRE(index.find(value(101ul)).empty());
    REQUIRE(index.find(value(0ul)).empty());

    REQUIRE(index.lower_bound(value(10ul)).size() == 9);
    REQUIRE(index.upper_bound(value(90ul)).size() == 10);

    for (int i = 2; i <= 100; i += 2) {
        index.remove(value(uint64_t(i)));
    }

    REQUIRE(index.find(value(2l)).empty());
    REQUIRE(index.lower_bound(value(10ul)).size() == 5);
    REQUIRE(index.upper_bound(value(90ul)).size() == 5);
}

TEST_CASE("index_disk::double") {
    std::filesystem::path path{"/tmp/index_disk/double"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = index_disk_t(path, core::type::float64);

    for (int i = 1; i <= 100; ++i) {
        index.insert(value(double(i)), document_id_t{gen_id(i)});
    }

    REQUIRE(index.find(value(1.)).size() == 1);
    REQUIRE(index.find(value(1.)).front() == document_id_t{gen_id(1)});
    REQUIRE(index.find(value(10.)).size() == 1);
    REQUIRE(index.find(value(10.)).front() == document_id_t{gen_id(10)});
    REQUIRE(index.find(value(100.)).size() == 1);
    REQUIRE(index.find(value(100.)).front() == document_id_t{gen_id(100)});
    REQUIRE(index.find(value(101.)).empty());
    REQUIRE(index.find(value(0.)).empty());

    REQUIRE(index.lower_bound(value(10.)).size() == 9);
    REQUIRE(index.upper_bound(value(90.)).size() == 10);

    for (int i = 2; i <= 100; i += 2) {
        index.remove(value(double(i)));
    }

    REQUIRE(index.find(value(2.)).empty());
    REQUIRE(index.lower_bound(value(10.)).size() == 5);
    REQUIRE(index.upper_bound(value(90.)).size() == 5);
}

TEST_CASE("index_disk::multi_values::int32") {
    std::filesystem::path path{"/tmp/index_disk/int32_multi"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = index_disk_t(path, core::type::int32);

    for (int i = 1; i <= 100; ++i) {
        for (int j = 0; j < 10; ++j) {
            index.insert(value(int64_t(i)), document_id_t{gen_id(1000 * j + i)});
        }
    }

    REQUIRE(index.find(value(1l)).size() == 10);
    REQUIRE(index.find(value(1l)).front() == document_id_t{gen_id(1)});
    REQUIRE(index.find(value(10l)).size() == 10);
    REQUIRE(index.find(value(10l)).front() == document_id_t{gen_id(10)});
    REQUIRE(index.find(value(100l)).size() == 10);
    REQUIRE(index.find(value(100l)).front() == document_id_t{gen_id(100)});
    REQUIRE(index.find(value(101l)).empty());
    REQUIRE(index.find(value(0l)).empty());

    REQUIRE(index.lower_bound(value(10l)).size() == 90);
    REQUIRE(index.upper_bound(value(90l)).size() == 100);

    for (int i = 2; i <= 100; i += 2) {
        for (int j = 5; j < 10; ++j) {
            index.remove(value(int64_t(i)), document_id_t{gen_id(1000 * j + i)});
        }
    }

    REQUIRE(index.find(value(2l)).size() == 5);
    REQUIRE(index.lower_bound(value(10l)).size() == 70);
    REQUIRE(index.upper_bound(value(90l)).size() == 75);
}
