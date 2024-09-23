#include <catch2/catch.hpp>
#include <components/tests/generaty.hpp>
#include <services/disk/index_disk.hpp>

using components::document::document_id_t;
using components::document::value_t;
using services::disk::index_disk_t;

std::string gen_str_new_value(int i, std::size_t size = 5) {
    auto s = std::to_string(i);
    while (s.size() < size) {
        s.insert(0, "0");
    }
    return s;
}

TEST_CASE("index_disk::string") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto tape = std::make_unique<impl::base_document>(&resource);
    auto new_value = [&](auto value) { return value_t{tape.get(), value}; };

    std::filesystem::path path{"/tmp/index_disk/string"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        index.insert(new_value(gen_id(i, &resource)), document_id_t{gen_id(i, &resource)});
    }

    REQUIRE(index.find(new_value(gen_id(1, &resource))).size() == 1);
    REQUIRE(index.find(new_value(gen_id(1, &resource))).front() == document_id_t{gen_id(1, &resource)});
    REQUIRE(index.find(new_value(gen_id(10, &resource))).size() == 1);
    REQUIRE(index.find(new_value(gen_id(10, &resource))).front() == document_id_t{gen_id(10, &resource)});
    REQUIRE(index.find(new_value(gen_id(100, &resource))).size() == 1);
    REQUIRE(index.find(new_value(gen_id(100, &resource))).front() == document_id_t{gen_id(100, &resource)});
    REQUIRE(index.find(new_value(gen_id(101, &resource))).empty());
    REQUIRE(index.find(new_value(gen_id(0, &resource))).empty());

    REQUIRE(index.lower_bound(new_value(gen_id(10, &resource))).size() == 9);
    REQUIRE(index.upper_bound(new_value(gen_id(90, &resource))).size() == 10);

    for (int i = 2; i <= 100; i += 2) {
        index.remove(new_value(gen_id(i)));
    }

    REQUIRE(index.find(new_value(gen_id(2, &resource))).empty());
    REQUIRE(index.lower_bound(new_value(gen_id(10, &resource))).size() == 5);
    REQUIRE(index.upper_bound(new_value(gen_id(90, &resource))).size() == 5);
}

TEST_CASE("index_disk::int32") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto tape = std::make_unique<impl::base_document>(&resource);
    auto new_value = [&](auto value) { return value_t{tape.get(), value}; };

    std::filesystem::path path{"/tmp/index_disk/int32"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        index.insert(new_value(int64_t(i)), document_id_t{gen_id(i, &resource)});
    }

    REQUIRE(index.find(new_value(1l)).size() == 1);
    REQUIRE(index.find(new_value(1l)).front() == document_id_t{gen_id(1, &resource)});
    REQUIRE(index.find(new_value(10l)).size() == 1);
    REQUIRE(index.find(new_value(10l)).front() == document_id_t{gen_id(10, &resource)});
    REQUIRE(index.find(new_value(100l)).size() == 1);
    REQUIRE(index.find(new_value(100l)).front() == document_id_t{gen_id(100, &resource)});
    REQUIRE(index.find(new_value(101l)).empty());
    REQUIRE(index.find(new_value(0l)).empty());

    REQUIRE(index.lower_bound(new_value(10l)).size() == 9);
    REQUIRE(index.upper_bound(new_value(90l)).size() == 10);

    for (int i = 2; i <= 100; i += 2) {
        index.remove(new_value(int64_t(i)));
    }

    REQUIRE(index.find(new_value(2l)).empty());
    REQUIRE(index.lower_bound(new_value(10l)).size() == 5);
    REQUIRE(index.upper_bound(new_value(90l)).size() == 5);
}

TEST_CASE("index_disk::uint32") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto tape = std::make_unique<impl::base_document>(&resource);
    auto new_value = [&](auto value) { return value_t{tape.get(), value}; };

    std::filesystem::path path{"/tmp/index_disk/uint32"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        index.insert(new_value(uint64_t(i)), document_id_t{gen_id(i, &resource)});
    }

    REQUIRE(index.find(new_value(1ul)).size() == 1);
    REQUIRE(index.find(new_value(1ul)).front() == document_id_t{gen_id(1, &resource)});
    REQUIRE(index.find(new_value(10ul)).size() == 1);
    REQUIRE(index.find(new_value(10ul)).front() == document_id_t{gen_id(10, &resource)});
    REQUIRE(index.find(new_value(100ul)).size() == 1);
    REQUIRE(index.find(new_value(100ul)).front() == document_id_t{gen_id(100, &resource)});
    REQUIRE(index.find(new_value(101ul)).empty());
    REQUIRE(index.find(new_value(0ul)).empty());

    REQUIRE(index.lower_bound(new_value(10ul)).size() == 9);
    REQUIRE(index.upper_bound(new_value(90ul)).size() == 10);

    for (int i = 2; i <= 100; i += 2) {
        index.remove(new_value(uint64_t(i)));
    }

    REQUIRE(index.find(new_value(2l)).empty());
    REQUIRE(index.lower_bound(new_value(10ul)).size() == 5);
    REQUIRE(index.upper_bound(new_value(90ul)).size() == 5);
}

TEST_CASE("index_disk::double") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto tape = std::make_unique<impl::base_document>(&resource);
    auto new_value = [&](auto value) { return value_t{tape.get(), value}; };

    std::filesystem::path path{"/tmp/index_disk/double"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        index.insert(new_value(double(i)), document_id_t{gen_id(i, &resource)});
    }

    REQUIRE(index.find(new_value(1.)).size() == 1);
    REQUIRE(index.find(new_value(1.)).front() == document_id_t{gen_id(1, &resource)});
    REQUIRE(index.find(new_value(10.)).size() == 1);
    REQUIRE(index.find(new_value(10.)).front() == document_id_t{gen_id(10, &resource)});
    REQUIRE(index.find(new_value(100.)).size() == 1);
    REQUIRE(index.find(new_value(100.)).front() == document_id_t{gen_id(100, &resource)});
    REQUIRE(index.find(new_value(101.)).empty());
    REQUIRE(index.find(new_value(0.)).empty());

    REQUIRE(index.lower_bound(new_value(10.)).size() == 9);
    REQUIRE(index.upper_bound(new_value(90.)).size() == 10);

    for (int i = 2; i <= 100; i += 2) {
        index.remove(new_value(double(i)));
    }

    REQUIRE(index.find(new_value(2.)).empty());
    REQUIRE(index.lower_bound(new_value(10.)).size() == 5);
    REQUIRE(index.upper_bound(new_value(90.)).size() == 5);
}

TEST_CASE("index_disk::multi_values::int32") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto tape = std::make_unique<impl::base_document>(&resource);
    auto new_value = [&](auto value) { return value_t{tape.get(), value}; };

    std::filesystem::path path{"/tmp/index_disk/int32_multi"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        for (int j = 0; j < 10; ++j) {
            index.insert(new_value(int64_t(i)), document_id_t{gen_id(1000 * j + i, &resource)});
        }
    }

    REQUIRE(index.find(new_value(1l)).size() == 10);
    REQUIRE(index.find(new_value(1l)).front() == document_id_t{gen_id(1, &resource)});
    REQUIRE(index.find(new_value(10l)).size() == 10);
    REQUIRE(index.find(new_value(10l)).front() == document_id_t{gen_id(10, &resource)});
    REQUIRE(index.find(new_value(100l)).size() == 10);
    REQUIRE(index.find(new_value(100l)).front() == document_id_t{gen_id(100, &resource)});
    REQUIRE(index.find(new_value(101l)).empty());
    REQUIRE(index.find(new_value(0l)).empty());

    REQUIRE(index.lower_bound(new_value(10l)).size() == 90);
    REQUIRE(index.upper_bound(new_value(90l)).size() == 100);

    for (int i = 2; i <= 100; i += 2) {
        for (int j = 5; j < 10; ++j) {
            index.remove(new_value(int64_t(i)), document_id_t{gen_id(1000 * j + i, &resource)});
        }
    }

    REQUIRE(index.find(new_value(2l)).size() == 5);
    REQUIRE(index.lower_bound(new_value(10l)).size() == 70);
    REQUIRE(index.upper_bound(new_value(90l)).size() == 75);
}
