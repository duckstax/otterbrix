#include <catch2/catch.hpp>

#include <wal/wal.hpp>

#include <msgpack.hpp>
#include <string>

using namespace services;
using namespace services::wal;
using namespace components::ql;

TEST_CASE("pack and unpack") {
    auto resource = std::pmr::synchronized_pool_resource();
    const std::string database = "test_database";
    const std::string collection = "test_collection";

    std::pmr::vector<components::document::document_ptr> documents(&resource);
    insert_many_t data(database, collection, std::move(documents));

    const crc32_t last_crc32 = 42;
    const wal::id_t wal_id = 21;

    buffer_t buffer;

    pack(buffer, last_crc32, wal_id, data);
    wal_entry_t<insert_many_t> entry(&resource);
    entry.size_ = read_size_impl(buffer, 0);

    auto start = sizeof(size_tt);
    auto finish = sizeof(size_tt) + entry.size_ + sizeof(crc32_t);
    auto storage = read_payload(buffer, int(start), int(finish));

    unpack<insert_many_t>(storage, entry, &resource);
    entry.crc32_ = read_crc32(storage, entry.size_);

    REQUIRE(entry.last_crc32_ == last_crc32);
    REQUIRE(entry.type_ == statement_type::insert_many);
    REQUIRE(entry.id_ == wal_id);
}
